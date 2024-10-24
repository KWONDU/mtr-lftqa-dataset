# Shot 1 : GEOquery subset

## NL query

    Which states border the states through which the Mississippi River traverses?

## SQL query

    SELECT border_info.border FROM border_info WHERE border_info.state_name IN(SELECT river.traverse FROM river WHERE river.river_name='mississippi');

## SQL query result (serialization)

    [row 1]: missouri [row 2]: tennessee [row 3]: mississippi [row 4]: louisiana [row 5]: texas [row 6]: oklahoma

## Statement

    States border the states through which the Mississippi River traverses are Mississippi, Louisiana, Texas and Oklahoma.
