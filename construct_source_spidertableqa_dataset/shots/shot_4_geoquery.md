# Shot 4 : GEOquery subset

## NL query

    Which rivers do not run through Tennessee?

## SQL query

    SELECT river.river_name FROM river WHERE river.river_name NOT IN(SELECT river.river_name FROM river WHERE river.traverse='tennessee');

## SQL query result (serialization)

    [header]: river_name [row 1]: missouri [row 2]: colorado [row 3]: ohio [row 4]: red [row 5]: arkansas [row 6]: canadian [row 7]: connecticut [row 8]: delaware [row 9]: little missouri [row 10]: snake [row 11]: chattahoochee [row 12]: cimarron [row 13]: green [row 14]: north platte [row 15]: potomac [row 16]: republican [row 17]: rio grande [row 18]: san juan [row 19]: wabash [row 20]: yellowstone [row 21]: allegheny [row 22]: bighorn [row 23]: cheyenne [row 24]: clark fork [row 25]: columbia [row 26]: dakota [row 27]: gila [row 28]: hudson [row 29]: neosho [row 30]: niobrara [row 31]: ouachita [row 32]: pearl [row 33]: pecos [row 34]: powder [row 35]: roanoke [row 36]: rock [row 37]: smoky hill [row 38]: south platte [row 39]: st. francis [row 40]: tombigbee [row 41]: washita [row 42]: wateree catawba [row 43]: white

## Statement

    Rivers do not run through Tennesse are Missouri, Colorado, Ohio, Red, Arkansas, Canadian, Connecticut, Delaware, Little missouri, Snake, Chattahoochee, Cimarron, Green, North platte, Potomac, Republican, Rio grande, San juan, Wabash, Yellowstone, Allegheny, Bighorn, Cheyenne, Clark fork, Columbia, Dakota, Gila, Hudson, Neosho, Niobrara, Ouachita, Pearl, Pecos, Powder, Roanoke, Rock, Smoky hill, South platte, St. francis, Tombigbee, Washita, Wateree catawba, and White.
