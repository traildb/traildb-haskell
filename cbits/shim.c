#include <traildb.h>

const tdb_event* shim_tdb_cursor_next(tdb_cursor* cursor)
{
    return tdb_cursor_next(cursor);
}

tdb_field shim_tdb_item_to_field(const tdb_item item)
{
    return tdb_item_field(item);
}

tdb_val shim_tdb_item_to_val(const tdb_item item)
{
    return tdb_item_val(item);
}

tdb_item shim_tdb_field_val_to_item(const tdb_field field, const tdb_val val)
{
    return tdb_make_item(field, val);
}

