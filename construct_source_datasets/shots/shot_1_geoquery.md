# Shot 1 : GEOquery subset

## NL query

    Which states border the states through which the Mississippi River traverses?

## SQL query

    SELECT border_info.border FROM border_info WHERE border_info.state_name IN(SELECT river.traverse FROM river WHERE river.river_name='mississippi');

## SQL query result (serialization)

    [header]: border [row 1]: missouri [row 2]: tennessee [row 3]: mississippi [row 4]: louisiana [row 5]: texas [row 6]: oklahoma [row 7]: wisconsin [row 8]: indiana [row 9]: kentucky [row 10]: iowa [row 11]: minnesota [row 12]: illinois [row 13]: nebraska [row 14]: south dakota [row 15]: ohio [row 16]: west virginia [row 17]: virginia [row 18]: arkansas [row 19]: north dakota [row 20]: alabama [row 21]: kansas [row 22]: north carolina [row 23]: georgia [row 24]: michigan

## Statement

    States border the states through which the Mississippi River traverses are Mississippi, Louisiana, Texas, Oklahoma, Wisconsin, Indiana, Kentucky, Iowa, Minnesota, Illinois, Nebraska, South dakota, Ohio, West virginia, Virginia, Arkansas, North dakota, Alabama, Kansas, North carolina, Georgia, and Michigan.
