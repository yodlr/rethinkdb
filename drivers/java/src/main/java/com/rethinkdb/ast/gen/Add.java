// Autogenerated by convert_protofile.py on 2015-05-07.
// Do not edit this file directly.
// The template for this file is located at:
// ../../../../../../../../templates/AstSubclass.java
package com.rethinkdb.ast.gen;

import com.rethinkdb.ast.helper.Arguments;
import com.rethinkdb.ast.helper.OptArgs;
import com.rethinkdb.ast.RqlAst;
import com.rethinkdb.proto.TermType;
import java.util.*;



public class Add extends RqlQuery {


    public Add(java.lang.Object arg) {
        this(new Arguments(arg), null);
    }
    public Add(Arguments args, OptArgs optargs) {
        this(null, args, optargs);
    }
    public Add(RqlAst prev, Arguments args, OptArgs optargs) {
        this(prev, TermType.ADD, args, optargs);
    }
    protected Add(RqlAst previous, TermType termType, Arguments args, OptArgs optargs){
        super(previous, termType, args, optargs);
    }


    /* Static factories */
    public static Add fromArgs(Object... args){
        return new Add(new Arguments(args), null);
    }


}
