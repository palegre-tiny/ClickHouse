#include <Interpreters/UnionSelectOptimizerVisitor.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

/*
option a)
┌─explain─────────────────────────────────────┐
│ SelectWithUnionQuery (children 1)           │ <- Replace this node
│  ExpressionList (children 1)                │
│   SelectQuery (children 2)                  │
│    ExpressionList (children 1)              │
│     Asterisk                                │
│    TablesInSelectQuery (children 1)         │
│     TablesInSelectQueryElement (children 1) │
│      TableExpression (children 1)           │
│       Subquery (children 1)                 │
│        SelectWithUnionQuery (children 1)    │ <- with this one
│            (whatever)                       │
└─────────────────────────────────────────────┘

option b)
┌─explain─────────────────────────────────────┐
│ SelectWithUnionQuery (children 1)           │
│  ExpressionList (children 2)                │
│   SelectQuery (children 2)                  │ <- Replace this node
│    ExpressionList (children 1)              │
│     Asterisk                                │
│    TablesInSelectQuery (children 1)         │
│     TablesInSelectQueryElement (children 1) │
│      TableExpression (children 1)           │
│       Subquery (children 1)                 │
│        SelectWithUnionQuery (children 1)    │
│         ExpressionList (children 1)         │
│          SelectQuery (children 1)           │ <- with this one
│           ExpressionList (children 1)       │
│            Literal UInt64_1 (alias n)       │
│   SelectQuery (children 2)                  │
│    ExpressionList (children 1)              │
│     Asterisk                                │
│    TablesInSelectQuery (children 1)         │
│     TablesInSelectQueryElement (children 1) │
│      TableExpression (children 1)           │
│       Subquery (children 1)                 │
│        SelectWithUnionQuery (children 1)    │
│         ExpressionList (children 1)         │
│          SelectQuery (children 1)           │
│           ExpressionList (children 1)       │
│            Literal UInt64_2 (alias n)       │
└─────────────────────────────────────────────┘


*/
namespace DB
{

void UnionSelectOptimizerVisitor::visit(ASTPtr & ast)
{
    if (ast->as<ASTSelectWithUnionQuery>() || ast->as<ASTSelectQuery>())
        while (visit_replace(ast))
            ;

    for (ASTPtr & child : ast->children)
        visit(child);
}

bool UnionSelectOptimizerVisitor::visit_replace(DB::ASTPtr & node)
{
    DB::ASTPtr query;
    DB::ASTPtr next = node;
    if (next->as<DB::ASTSelectWithUnionQuery>())
    {
        if (!move_next<DB::ASTExpressionList>(next))
            return false;

        if (!move_next<DB::ASTSelectQuery>(next))
            return false;

        for (DB::ASTPtr & child : next->children)
        {
            if (child->as<DB::ASTExpressionList>())
            {
                next = child;
                if (!move_next<DB::ASTAsterisk>(next))
                    return false;
                continue;
            }
            if (child->as<DB::ASTTablesInSelectQuery>())
            {
                next = child;
                if (!move_next<DB::ASTTablesInSelectQueryElement>(next))
                    return false;

                if (!move_next<DB::ASTTableExpression>(next))
                    return false;

                if (!move_next<DB::ASTSubquery>(next))
                    return false;

                if (!move_next<DB::ASTSelectWithUnionQuery>(next))
                    return false;

                query = next;
                continue;
            }
            else
                return false;
        }
    }
    else if (next->as<DB::ASTSelectQuery>())
    {
        if (next->children.size() != 2)
            return false;

        for (DB::ASTPtr & child : next->children)
        {
            if (child->as<DB::ASTExpressionList>())
            {
                next = child;
                if (!move_next<DB::ASTAsterisk>(next))
                    return false;
                continue;
            }
            if (child->as<DB::ASTTablesInSelectQuery>())
            {
                next = child;
                if (!move_next<DB::ASTTablesInSelectQueryElement>(next))
                    return false;

                if (!move_next<DB::ASTTableExpression>(next))
                    return false;

                if (!move_next<DB::ASTSubquery>(next))
                    return false;

                if (!move_next<DB::ASTSelectWithUnionQuery>(next))
                    return false;

                if (!move_next<DB::ASTExpressionList>(next))
                    return false;

                if (!move_next<DB::ASTSelectQuery>(next))
                    return false;

                query = next;
                continue;
            }
            else
                return false;
        }
    }

    node.swap(query);
    return true;
}

}
