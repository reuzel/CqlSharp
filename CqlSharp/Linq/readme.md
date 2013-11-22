#Design of the Cql Linq 

## Context and Table

## Cql Expression Trees
The general concept behind this Linq provider implementation is that a regular Linq expression
tree is transformed into an expression tree that contain Cql expressions such as select, relation
term, etc. The Cql Expression are derived directly from the select grammer as defined in Cql 3.1.1:

```
<select-stmt> ::= SELECT <select-clause>
                  FROM <tablename>
                  ( WHERE <where-clause> )?
                  ( ORDER BY <order-by> )?
                  ( LIMIT <integer> )?
                  ( ALLOW FILTERING )?

<select-clause> ::= DISTINCT? <selection-list>
                  | COUNT '(' ( '*' | '1' ) ')' (AS <identifier>)?

<selection-list> ::= <selector> (AS <identifier>)? ( ',' <selector> (AS <identifier>)? )*
                   | '*'

<selector> ::= <identifier>
             | WRITETIME '(' <identifier> ')'
             | TTL '(' <identifier> ')'
             | <function> '(' (<selector> (',' <selector>)*)? ')'

<where-clause> ::= <relation> ( AND <relation> )*

<relation> ::= <identifier> ('=' | '<' | '>' | '<=' | '>=') <term>
             | <identifier> IN '(' ( <term> ( ',' <term>)* )? ')'
             | TOKEN '(' <identifier> ( ',' <identifer>)* ')' ('=' | '<' | '>' | '<=' | '>=') <term>

<order-by> ::= <ordering> ( ',' <odering> )*
<ordering> ::= <identifer> ( ASC | DESC )?

<identifier> ::= any quoted or unquoted identifier, excluding reserved keywords

<term> ::= <constant>
               | <collection-literal>
               | <variable>
               | <function> '(' (<term> (',' <term>)*)? ')'

 <collection-literal> ::= <map-literal>
                         | <set-literal>
                         | <list-literal>
         <map-literal> ::= '{' ( <term> ':' <term> ( ',' <term> ':' <term> )* )? '}'
         <set-literal> ::= '{' ( <term> ( ',' <term> )* )? '}'
        <list-literal> ::= '[' ( <term> ( ',' <term> )* )? ']'
```