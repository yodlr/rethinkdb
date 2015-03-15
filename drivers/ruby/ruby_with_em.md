# Accessing RethinkDB with EventMachine

## Basic Usage

The simplest way to access RethinkDB with EventMachine is to call
`em_run` with a block.  If RethinkDB returns a sequence (an array or a
stream), the block will be called with each element of that sequence.
If RethinkDB returns anything else, the block will be called once with
that value.

#### Example 1: iterating over a stream

```rb
# Insert some data.
r.table('test').insert([{id: 0}, {id: 1}, {id: 2}]).run($conn)
EM.run {
  # Print every row in the table.
  r.table('test').order_by(index: 'id').em_run($conn) {|row|
    p row
  }
}
```

Will print this:

```rb
{"id"=>0}
{"id"=>1}
{"id"=>2}
```

#### Example 2: iterating over an array

```rb
EM.run {
  # Print an array.
  r.expr([1, 2, 3]).em_run($conn) {|n|
    p n
  }
}
``

Will print this:

```rb
1
2
3
```

#### Example 3: accessing a single row

```rb
# Insert some data.
r.table('test').insert([{id: 0}, {id: 1}, {id: 2}]).run($conn)
EM.run {
  # Print a single row.
  r.table('test').get(0).em_run($conn) {|row|
    p row
  }
}
```

Will print this:

```rb
{"id"=>0}
```

## Basic Error Handling

If you pass `em_run` a block that only accepts a single argument,
RethinkDB's EM adapter will handle errors by re-throwing them.  You
can also handle errors in the block by passing `em_run` a block that
accepts two arguments.  If you do so, then the block will either be
called with `nil` as its first argument and data as its second
argument, or with an error as its first argument and `nil` as its
second argument.

Note that it's possible for the block to be passed data many times,
and then be passed an error.  For example, this can happen if you lose
availability in the middle of a long read.

#### Example 1: re-thrown errors

```rb
EM.run {
  r.table('non_existent').em_run($conn) {|row|
    p row
  }
}
```

Will produce an error:

```rb
RethinkDB::RqlRuntimeError: Table `test.non_existent` does not exist.
Backtrace:
r.table("non_existent")
^^^^^^^^^^^^^^^^^^^^^^^
```

#### Example 2: handling errors in the block

```rb
EM.run {
  r.table('non_existent').em_run($conn) {|err, row|
    if err
      p [:err, err.to_s]
    else
      p [:row, row]
    end
  }
}
```

Will print:

```rb
[:err, "Table `test.non_existent` does not exist.\nBacktrace:\nr.table(\"non_existent\")\n^^^^^^^^^^^^^^^^^^^^^^^"]
```

And

```rb
# Insert some data.
r.table('test').insert([{id: 0}, {id: 1}, {id: 2}]).run($conn)
EM.run {
  # Print every row in the table.
  r.table('test').order_by(index: 'id').em_run($conn) {|err, row|
    if err
      p [:err, err.to_s]
    else
      p [:row, row]
    end
  }
}
```

Will print:

```rb
[:row, {"id"=>0}]
[:row, {"id"=>1}]
[:row, {"id"=>2}]
```

## Advanced Usage

More precise control is provided by a superclass `RethinkDB::Handler`.
You can write a class that inherits from `RethinkDB::Handler` and
overrides certain methods, then pass an instance of that class to
`em_run`.  (If you instead pass the name of the class, the RethinkDB
event machine adapter will instantiatie it for you with no arguments
and use that.)

Here's a simple example of a handler that prints things:

```rb
class Printer < RethinkDB::Handler
  def on_open
    p :open
  end
  def on_close
    p :closed
  end
  def on_error(err)
    p [:err, err.to_s]
  end
  def on_val(val)
    p [:val, val]
  end
end
```

#### Example 1: handling a stream


```rb
# Insert some data.
r.table('test').insert([{id: 0}, {id: 1}, {id: 2}]).run($conn)
EM.run {
  # Print every row in the table.
  r.table('test').order_by(index: 'id').em_run($conn, Printer)
}
```

Will print this:

```rb
:open
[:val, {"id"=>0}]
[:val, {"id"=>1}]
[:val, {"id"=>2}]
:closed
```

## Granular Advanced Usage

Sometimes you want to treat atoms differently from sequences, or to
distinguish streams from arrays.  You can do that by defining
`on_array`, `on_atom`, and `on_stream_val`.  (If `on_array` isn't
defined, arrays will be treated the same as streams and
`on_stream_val` or `on_val` will be called for every element of the
array.)

```rb
class Printer < RethinkDB::Handler
  def on_open
    p :open
  end
  def on_close
    p :closed
  end
  def on_error(err)
    p [:err, err.to_s]
  end
  def on_atom(atom)
    p [:atom, atom]
  end
  def on_array(array)
    p [:array, array]
  end
  def on_stream_val(val)
    p [:stream_val, val]
  end
end
```

#### Example 1: handling granular results

```
# Insert some data.
r.table('test').insert([{id: 0}, {id: 1}, {id: 2}]).run($conn)
EM.run {
  # Print every row in the table.
  r.table('test').order_by(index: 'id').em_run($conn, Printer)
  # Print an array.
  r.expr([1, 2, 3]).em_run($conn, Printer)
  # Print a single row.
  r.table('test').get(0).em_run($conn, Printer)
}
```

Will print this:

```
:open
[:stream_val, {"id"=>0}]
[:stream_val, {"id"=>1}]
[:stream_val, {"id"=>2}]
:closed
:open
[:array, [1, 2, 3]]
:closed
:open
[:atom, {"id"=>0}]
:closed
```

(Note that when you register multiple callbacks, the order they're
called in isn't guaranteed -- it would be perfectly legal for this to
have printed the result of `r.expr([1, 2, 3])` first.)

## Changefeeds

Changefeeds can be treated like normal streams.  If you pass a block
to `em_run`, that block will be called with each document in the
change stream.  If you pass a `Handler` that defines `on_val` or
`on_stream_val`, that function will be called with each document in
the change stream.

If you want more granular control, there are also several changefeed
specific functions that can be defined:

* `on_initial_val` -- if you're subscribed to a changefeed that
  returns initial values (`.get.changes` and `.order_by.limit.changes`
  right now), those initial values will be passed to this function.
* `on_change` -- changes will be passed to this function.
* `on_change_error` -- sometimes the change stream includes documents
  specifying errors that don't abort the feed (e.g. if the client is
  reading too slowly and the server was forced to discard changes).
  Those will be passed to this function if it's defined.
* `on_state` -- sometimes the change stream includes documents
  specifying the state of the stream.  Those will be passed to this
  function if it's defined.

Here's our printer class with these functions defined:

```rb
class Printer < RethinkDB::Handler
  def on_open
    p :open
  end
  def on_close
    p :closed
  end
  def on_error(err)
    p [:err, err.to_s]
  end

  def on_change_error(err_str)
    p [:change_error, err_str]
  end
  def on_initial_val(val)
    p [:initial_val, val]
  end
  def on_state(state)
    p [:state, state]
  end
  def on_change(old_val, new_val)
    p [:change, old_val, new_val]
  end
end
```

#### Example 1: `.order_by.limit.changes`

```rb
# Insert some data.
r.table('test').insert([{id: 0}, {id: 1}, {id: 2}]).run($conn)
EM.run {
  # Subscribe to changes on the two smallest elements.
  r.table('test').order_by(index: 'id').limit(2).changes.em_run($conn, Printer)
}
```

Will print this:

```rb
:open
[:state, "initializing"]
[:initial_val, {"id"=>1}]
[:initial_val, {"id"=>0}]
[:state, "ready"]
```

If you then insert a new row with an `id` of 0.5:

```rb
r.table('test').insert({id: 0.5}).run($conn)
```

It will replace `{id: 1}` in the set with `{id: 0.5}`:

```rb
[:change, {"id"=>1}, {"id"=>0.5}]
```

If the table is then dropped:

```rb
r.table_drop('test').run($conn)
```

The changefeed will receive an error and be closed:
```rb
[:err, "Changefeed aborted (table unavailable).\nBacktrace:\nr.table(\"test\").changes\n^^^^^^^^^^^^^^^^^^^^^^^"]
:closed
```

## Stopping a stream

Streams are stopped when they are exhausted or when EventMachine
stops.  If you're using a `Handler`, you can also stop all streams
using that handler by calling `Handler::stop`.

#### Example 1: printing the first 5 changes

```rb
class Printer < RethinkDB::Handler
  def initialize(max)
    @counter = max
    stop if @counter <= 0
  end
  def on_open
    # Once the changefeed is open, insert 10 rows.
    r.table('test').insert([{}]*10).run($conn, noreply: true)
  end
  def on_val(val)
    # Every time we print a change, decrement `@counter` and stop if we hit 0.
    p val
    @counter -= 1
    stop if @counter <= 0
  end
end

EM.run {
  r.table('test').changes.em_run($conn, Printer.new(5))
}
```

Will print this:

```
{"old_val"=>nil, "new_val"=>{"id"=>"07cb420f-905b-4cbf-bd82-b4885babe1e1"}}
{"old_val"=>nil, "new_val"=>{"id"=>"4517ba6d-5511-405d-8991-682ca0a375fd"}}
{"old_val"=>nil, "new_val"=>{"id"=>"c5f1074e-7a72-403c-bfe9-9d7d7de4a2e9"}}
{"old_val"=>nil, "new_val"=>{"id"=>"f41daf81-fa00-40be-97e8-bba02fedd9ae"}}
{"old_val"=>nil, "new_val"=>{"id"=>"1975449c-9cd7-4a3b-b027-1b9bdabf1299"}}
```

#### Example 2: stopping a changefeed based on another changefeed

```rb
class Printer < RethinkDB::Handler
  def on_val(val)
    p val
  end
end

EM.run {
  printer = Printer.new
  r.table('test').changes.em_run($conn, Printer)
  r.table('commands').changes['new_val']['stop'].em_run($conn) {|should_stop|
    printer.stop if should_stop
  }
}
```

Will print changes to the table `test` until you run
`r.table('commands').insert({stop: true})`.
