# Shot 2 : GEOquery subset

## NL query

    What cities are in California?

## SQL query

    SELECT city.city_name FROM city WHERE city.state_name='california';

## SQL query result (serialization)

    [header]: city_name [row 1]: los angeles [row 2]: san diego [row 3]: san francisco [row 4]: san jose [row 5]: long beach [row 6]: oakland [row 7]: sacramento [row 8]: anaheim [row 9]: fresno [row 10]: santa ana [row 11]: riverside [row 12]: huntington beach [row 13]: stockton [row 14]: glendale [row 15]: fremont [row 16]: torrance [row 17]: garden grove [row 18]: san bernardino [row 19]: pasadena [row 20]: east los angeles [row 21]: oxnard [row 22]: modesto [row 23]: sunnyvale [row 24]: bakersfield [row 25]: concord [row 26]: berkeley [row 27]: fullerton [row 28]: inglewood [row 29]: hayward [row 30]: pomona [row 31]: orange [row 32]: ontario [row 33]: santa monica [row 34]: santa clara [row 35]: citrus heights [row 36]: norwalk [row 37]: burbank [row 38]: chula vista [row 39]: santa rosa [row 40]: downey [row 41]: costa mesa [row 42]: compton [row 43]: carson [row 44]: salinas [row 45]: west covina [row 46]: vallejo [row 47]: el monte [row 48]: daly city [row 49]: thousand oaks [row 50]: san mateo [row 51]: simi valley [row 52]: oceanside [row 53]: richmond [row 54]: lakewood [row 55]: santa barbara [row 56]: el cajon [row 57]: ventura [row 58]: westminster [row 59]: whittier [row 60]: south gate [row 61]: alhambra [row 62]: buena park [row 63]: san leandro [row 64]: alameda [row 65]: newport beach [row 66]: escondido [row 67]: irvine [row 68]: mountain view [row 69]: fairfield [row 70]: redondo beach [row 71]: scotts valley

## Statement

    Cities in California are Los angeles, San diego, San francisco, San jose, Long beach, Oakland, Sacramento, Anaheim, Fresno, Santa ana, Riverside, Huntington beach, Stockton, Glendale, Fremont, Torrance, Garden grove, San bernardino, Pasadena, East los angeles, Oxnard, Modesto, Sunnyvale, Bakersfield, Concord, Berkeley, Fullerton, Inglewood, Hayward, Pomona, Orange, Ontario, Santa monica, Santa clara, Citrus heights, Norwalk, Burbank, Chula vista, Santa rosa, Downey, Costa mesa, Compton, Carson, Salinas, West covina, Vallejo, El monte, Daly city, Thousand oaks, San mateo, Simi valley, Oceanside, Richmond, Lakewood, Santa barbara, El cajon, Ventura, Westminster, Whittier, South gate, Alhambra, Buena park, San leandro, Alameda, Newport beach, Escondido, Irvine, Mountain view, Fairfield, Redondo beach, and Scotts valley.
