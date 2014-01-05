#Design of Linq2Cql
Most of the ideas on how this provider is created stems from a blog series from Matt Warren: 
[LINQ: Building an IQueryable provider series](http://blogs.msdn.com/b/mattwar/archive/2008/11/18/linq-links.aspx)

The general concept behind this Linq provider implementation is that a regular Linq expression
tree is transformed into an expression tree that contains Cql expressions such as select, relation
term, etc. Using cql specific expression types the original query can be adapted to contain references
(expressions) to column values, allowing reasoning about the meaning of the different operations.

The final expression tree is translated into a Cql query as well as delegate that transforms the query
results into the required object structure.

## Context and Table
CqlContext and CqlTable are the classes that form the core of the provider. CqlContext is the 
IQueryProvider, while CqlTable is the IQueryable.

## Supported Operations
The following operations are supported:

* Select
* Where
* Any
* Count
* LongCount
* First
* FirstOrDefault
* Single
* SingleOrDefault
* ToList
* OrderBy
* OrderByDescending
* ThenBy
* ThenByDescending
* Take

Take must come after clause that would introduce a where clause in the resulting Cql

## Cql Expression types
The Cql Expression types are derived directly from the select grammar as defined in Cql 3.1.1.

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