#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"

class TableHeap;

class TableIterator
{
public:
    // you may define your own constructor based on your member variables
    explicit TableIterator();

    explicit TableIterator(TableHeap *);

    explicit TableIterator(TableHeap *, RowId &);

    /* explicit */ TableIterator(const TableIterator &other);

    virtual ~TableIterator();

    bool operator==(const TableIterator &itr) const;

    bool operator!=(const TableIterator &itr) const;

    const Row &operator*();

    Row *operator->();

    TableIterator &operator=(const TableIterator &itr) noexcept;

    TableIterator &operator++();

    TableIterator operator++(int);

    inline RowId GetRid() { return rid;}

private:
    // add your own private member variables here
    TableHeap *tables = nullptr;
    RowId rid{INVALID_ROWID};
    Row *row = new Row();
    void FindNextRow(RowId &);
};

#endif // MINISQL_TABLE_ITERATOR_H
