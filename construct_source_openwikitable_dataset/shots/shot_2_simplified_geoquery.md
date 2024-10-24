# Shot 2 : GEOquery subset

## NL query

    Which rivers do not run through Tennessee?

## SQL query

    SELECT river.river_name FROM river WHERE river.river_name NOT IN(SELECT river.river_name FROM river WHERE river.traverse='tennessee');

## SQL query result (serialization)

    [row 1]: missouri [row 2]: colorado [row 3]: ohio [row 4]: red [row 5]: arkansas

## Statement

    Rivers do not run through Tennesse are Missouri, Colorado, Ohio, Red and Arkansas.
