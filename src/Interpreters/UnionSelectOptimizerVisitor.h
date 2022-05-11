#pragma once

#include <Parsers/IAST.h>

namespace DB
{

struct UnionSelectOptimizerVisitor
{
public:
    static void visit(ASTPtr & ast);

private:
    template <typename T>
    static bool move_next(DB::ASTPtr & item)
    {
        if (item->children.size() != 1 || !item->children.at(0)->as<T>())
        {
            return false;
        }
        item = item->children.at(0);
        return true;
    }


    static bool visit_replace(ASTPtr & node);
};
}
