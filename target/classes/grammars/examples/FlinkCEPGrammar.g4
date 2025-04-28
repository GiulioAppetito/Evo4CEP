grammar FlinkCEPGrammar;

pattern
    : ('begin')? eventSequence withinClause?
    ;

eventSequence
    : event (binaryOp event)*
    ;

event
    : IDENTIFIER condition? quantifier?
    ;

binaryOp
    : 'next'
    | 'followedBy'
    | 'followedByAny'
    ;

quantifier
    : 'times' INT
    | 'oneOrMore'
    | 'optional'
    ;

withinClause
    : 'within' DURATION
    ;

condition
    : 'where' conditionExpression
    ;

conditionExpression
    : conditionAtom (conditionOp conditionExpression)?
    ;

conditionOp
    : 'AND' | 'OR'
    ;

conditionAtom
    : variable relationalOp value
    ;

relationalOp
    : '==' | '!=' | '<' | '>' | '<=' | '>='
    ;

value
    : 'true'
    | 'false'
    | IDENTIFIER
    | INT
    | FLOAT
    | STRING
    ;

variable
    : IDENTIFIER
    ;

DURATION
    : INT 's'   // seconds
    | INT 'm'   // minutes
    | INT 'h'   // hours
    ;

INT
    : [0-9]+
    ;

FLOAT
    : [0-9]+ '.' [0-9]+
    ;

STRING
    : '"' (~["\\] | '\\' .)* '"'
    ;

IDENTIFIER
    : [a-zA-Z_][a-zA-Z_0-9]*
    ;

WS
    : [ \t\r\n]+ -> skip
    ;
