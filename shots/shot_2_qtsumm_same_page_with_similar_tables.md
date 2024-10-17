# Shot 2 : same page, similar tables

## Gold table set

### Table 1

**Visualization**

    Table id: e5ed2d8005238f50b689f239d335411f70cded82553581295f4874f58a85e915
    metadata: List Of The Busiest Airports In The United States - Ranking by the amount of cargo
    Rank | Airport Name                                | Location                 | Iata Code | Tonnes   | % Chg 2010 / 11
    ----------------------------------------------------------------------------------------------------------------------
    1    | Memphis International Airport               | Memphis , Tennessee      | Mem       | 3916410  | 0 0.0%         
    2    | Ted Stevens Anchorage International Airport | Anchorage , Alaska       | Anc       | 2543105  | 0 3.9%         
    3    | Louisville International Airport            | Louisville , Kentucky    | Sdf       | 2188422  | 0 1.0%         
    4    | Miami International Airport                 | Miami , Florida          | Mia       | 1841929  | 0 0.3%         
    5    | Los Angeles International Airport           | Los Angeles , California | Lax       | 1681611  | 0 3.8%         
    6    | John F Kennedy International Airport        | Queens , New York        | Jfk       | 1348992  | 0 0.5%         
    7    | O'Hare International Airport                | Chicago , Illinois       | Ord       | 1311622  | 0 4.7%         
    8    | Indianapolis International Airport          | Indianapolis             | Ind       | 0 971664 | 0 4.0%         
    9    | Newark Liberty International Airport        | Newark , New Jersey      | Ewr       | 0 813209 | 0 5.0%         

**Serialization**

    Table 1 [title]: List Of The Busiest Airports In The United States - Ranking by the amount of cargo [header]: Rank | Airport Name | Location | Iata Code | Tonnes | % Chg 2010 / 11 [row 1]: 1 | Memphis International Airport | Memphis , Tennessee | Mem | 3916410 | 0 0.0% [row 2]: 2 | Ted Stevens Anchorage International Airport | Anchorage , Alaska | Anc | 2543105 | 0 3.9% [row 3]: 3 | Louisville International Airport | Louisville , Kentucky | Sdf | 2188422 | 0 1.0% [row 4]: 4 | Miami International Airport | Miami , Florida | Mia | 1841929 | 0 0.3% [row 5]: 5 | Los Angeles International Airport | Los Angeles , California | Lax | 1681611 | 0 3.8% [row 6]: 6 | John F Kennedy International Airport | Queens , New York | Jfk | 1348992 | 0 0.5% [row 7]: 7 | O'Hare International Airport | Chicago , Illinois | Ord | 1311622 | 0 4.7% [row 8]: 8 | Indianapolis International Airport | Indianapolis | Ind | 0 971664 | 0 4.0% [row 9]: 9 | Newark Liberty International Airport | Newark , New Jersey | Ewr | 0 813209 | 0 5.0%

### Table 2

**Visualization**

    Table id: de8eafd6529310457cbb967e43024449e0185103fd276fe08864facc3c1e552f
    metadata: List Of The Busiest Airports In The United States - Ranking by the number of passengers
    Rank | Airport Name                                       | Location                      | Iata Code | Passengers | % Chg 2009 / 10
    ------------------------------------------------------------------------------------------------------------------------------------
    1    | John F Kennedy International Airport               | Queens , New York             | Jfk       | 22702882   | 5.8%           
    2    | Miami International Airport                        | Miami , Florida               | Mia       | 16207353   | 5.3%           
    3    | Los Angeles International Airport                  | Los Angeles , California      | Lax       | 15505566   | 4.7%           
    4    | Newark Liberty International Airport               | Newark , New Jersey           | Ewr       | 11372186   | 6.6%           
    5    | O'Hare International Airport                       | Chicago , Illinois            | Ord       | 10426287   | 2.5%           
    6    | Hartsfield - Jackson Atlanta International Airport | Atlanta , Georgia             | Atl       | 9120974    | 4.1%           
    7    | San Francisco International Airport                | San Mateo County , California | Sfo       | 8476172    | 7.4%           
    8    | George Bush Intercontinental Airport               | Houston , Texas               | Iah       | 8279513    | 9.5%           
    9    | Washington Dulles International Airport            | Dulles , Virginia             | Iad       | 6167501    | 9.5%           

**Serialization**

    Table 2 [title]: List Of The Busiest Airports In The United States  - Ranking by the number of passengers [header]: Rank | Airport Name | Location | Iata Code | Passengers | % Chg 2009 / 10 [row 1]: 1 | John F Kennedy International Airport | Queens , New York | Jfk | 22702882 | 5.8% [row 2]: 2 | Miami International Airport | Miami , Florida | Mia | 16207353 | 5.3% [row 3]: 3 | Los Angeles International Airport | Los Angeles , California | Lax | 15505566 | 4.7% [row 4]: 4 | Newark Liberty International Airport | Newark , New Jersey | Ewr | 11372186 | 6.6% [row 5]: 5 | O'Hare International Airport | Chicago , Illinois | Ord | 10426287 | 2.5% [row 6]: 6 | Hartsfield - Jackson Atlanta International Airport | Atlanta , Georgia | Atl | 9120974 | 4.1% [row 7]: 7 | San Francisco International Airport | San Mateo County , California | Sfo | 8476172 | 7.4% [row 8]: 8 | George Bush Intercontinental Airport | Houston , Texas | Iah | 8279513 | 9.5% [row 9]: 9 | Washington Dulles International Airport | Dulles , Virginia | Iad | 6167501 | 9.5%

## QA pairs - Entailed statements

### About table 1

**QA pair 1 - Entailed statement 1**

    Q.  What are the top three busiest airports in the United States? Summarize the basic information of these airports.
    A.  The top three busiest airports in the United States are Memphis International Airport, Ted Stevens Anchorage International Airport and Louisville International Airport. Memphis International Airport is located in Memphis, Tennessee with an IATA code of Mem and has handled 3916410 tonnes of cargo in 2010/11 with no change from the previous year. Ted Stevens Anchorage International Airport is located in Anchorage, Alaska with an IATA code of Anc and has handled 2543105 tonnes of cargo in 2010/11 with a 3.9% change from the previous year. Louisville International Airport is located in Louisville, Kentucky with an IATA code of SDF and has handled 2188422 tonnes of cargo in 2010/11 with a 1.0% change from the previous year.
    ---
    S.  The top three busiest airports in the United States are Memphis International Airport, Ted Stevens Anchorage International Airport, and Louisville International Airport. Memphis International Airport, located in Memphis, Tennessee, with an IATA code of MEM, handled 3,916,410 tonnes of cargo in 2010/11, with no change from the previous year. Ted Stevens Anchorage International Airport, located in Anchorage, Alaska, with an IATA code of ANC, handled 2,543,105 tonnes of cargo in 2010/11, with a 3.9% increase from the previous year. Louisville International Airport, located in Louisville, Kentucky, with an IATA code of SDF, handled 2,188,422 tonnes of cargo in 2010/11, with a 1.0% increase from the previous year.
    ---
    NQ. What are the top three busiest airports in the United States?

**QA pair 2 - Entailed statement 2**

    Q.  Summarize the airports located in the states of Illinois and California.
    A.  The airports located in the states of Illinois and California are some of the busiest in the United States. The O'Hare International Airport in Chicago, Illinois is ranked 7th on the list with a total of 1311622 tonnes moved in 2010-2011 and a 4.7% increase over the same time period. Los Angeles International Airport (LAX) in California is 5th on the list with a total of 1681611 tonnes moved and a 3.8% increase over 2010-2011.
    ---
    S.  The airports located in Illinois and California include O’Hare International Airport in Chicago, Illinois, and Los Angeles International Airport in California. O’Hare International Airport is ranked 7th among the busiest airports in the United States, having moved 1,311,622 tonnes of cargo in 2010-2011, with a 4.7% increase from the previous year. Los Angeles International Airport (LAX) is ranked 5th, handling 1,681,611 tonnes of cargo during the same period, with a 3.8% increase over the previous year.
    ---
    NQ. What are the airports located in the states of Illinois and California?

**QA pair 3 - Entailed statement 3**

    Q.  Which airport may be the largest, and why?
    A.  The largest airport in the United States may be the Memphis International Airport located in Tennessee. This is indicated by its rank at number one, as well as its tonnes of 3916410, which is significantly more than any other airport on this list.
    ---
    S.  The largest airport in the United States may be Memphis International Airport in Tennessee. This is supported by its rank as the number one busiest airport, handling 3,916,410 tonnes of cargo, which is significantly more than any other airport on the list.
    ---
    NQ. Which airport may be the largest?

### About table 2

**QA pair 1 - Entailed statement 1**

    Q.  Summarize the airports located in New York and California.
    A.  New York and California are home to some of the busiest airports in the United States. The first airport on this list from these states is John F Kennedy International Airport located in Queens New York. It is the busiest airport in the US with 22,702,882 passengers passing through its gates in 2010, which was a 5.8% increase from 2009. From California can be found Los Angeles International Airport located in Los Angeles and San Francisco International Airport located in San Mateo County. These two airports had 15,505,566 and 8,476,172 passengers respectively in 2010, a 4.7% and 7.4% increase respectively since 2009.
    ---
    S.  The airports located in New York and California include John F. Kennedy International Airport in Queens, New York, and Los Angeles International Airport and San Francisco International Airport in California. John F. Kennedy International Airport is ranked 1st among the busiest airports in the United States, with 22,702,882 passengers in 2010, reflecting a 5.8% increase from the previous year. Los Angeles International Airport (LAX) is ranked 3rd, handling 15,505,566 passengers in 2010, with a 4.7% increase, while San Francisco International Airport (SFO) is ranked 7th, serving 8,476,172 passengers, with a 7.4% increase from the previous year.
    ---
    NQ. What the airports located in New York and California?

**QA pair 2 - Entailed statement 2**

    Q.  Summarize the basic information of the airports located in Georgia and Virginia.
    A.  The two airports located in Georgia and Virginia are Hartsfield-Jackson Atlanta International Airport and Washington Dulles International Airport. Located in Atlanta, Georgia, Hartsfield-Jackson is the busiest airport on this list with 9,120,974 passengers traveling through it in 2009. It saw a 4.1 percent change in passenger numbers that same year. Washington Dulles International Airport serves the state of Virginia with 6,167,501 travelers recorded in 2009, which represented a 9.5 percent increase from the prior year.
    ---
    S.  The airports located in Georgia and Virginia include Hartsfield-Jackson Atlanta International Airport in Atlanta, Georgia, and Washington Dulles International Airport in Dulles, Virginia. Hartsfield-Jackson Atlanta International Airport is ranked 6th among the busiest airports in the United States, with 9,120,974 passengers in 2009, reflecting a 4.1% increase from the previous year. Washington Dulles International Airport is ranked 9th, serving 6,167,501 passengers in 2009, with a 9.5% increase from the prior year.
    ---
    NQ. What are the airports located in Georgia and Virginia?

## Annotation

**QA pair 1 : Easy # 1-1**

    Q.  What are the top three busiest U.S. airports by both cargo throughput and passenger traffic in the list of the busiest airports in the United States? Summarize the basic information of the airports.
    A.  The top three busiest U.S. airports by cargo throughput in 2010/11 are Memphis International Airport, Ted Stevens Anchorage International Airport, and Louisville International Airport. Memphis International Airport, located in Memphis, Tennessee (MEM), handled 3,916,410 tonnes of cargo with no change from the previous year. Ted Stevens Anchorage International Airport, located in Anchorage, Alaska (ANC), handled 2,543,105 tonnes of cargo with a 3.9% increase. Louisville International Airport, located in Louisville, Kentucky (SDF), handled 2,188,422 tonnes of cargo with a 1.0% increase from the previous year. For passenger traffic, the top three busiest U.S. airports in 2009/10 are John F. Kennedy International Airport, Miami International Airport, and Los Angeles International Airport. John F. Kennedy International Airport, located in Queens, New York (JFK), handled 22,702,882 passengers with a 5.8% increase. Miami International Airport, located in Miami, Florida (MIA), handled 16,207,353 passengers with a 5.3% increase. Los Angeles International Airport, located in Los Angeles, California (LAX), handled 15,505,566 passengers with a 4.7% increase.

**QA pair 2 : Easy # 2-1**

    Q.  Summarize the airports located in New York and California in the list of the busiest airports in the United States.
    A.  The airports located in New York and California include John F. Kennedy International Airport in Queens, New York, and Los Angeles International Airport and San Francisco International Airport in California. John F. Kennedy International Airport (JFK) is ranked 6th among the busiest cargo throughput airports in the United States, handling 1,348,992 tonnes of cargo in 2010/11 with a 0.5% increase from the previous year. It is also ranked 1st in terms of passenger traffic, serving 22,702,882 passengers in 2009/10, with a 5.8% increase. Los Angeles International Airport (LAX) is ranked 5th in cargo throughput, handling 1,681,611 tonnes in 2010/11 with a 3.8% increase. It is ranked 3rd in passenger traffic, serving 15,505,566 passengers in 2009/10 with a 4.7% increase. San Francisco International Airport (SFO) is ranked 7th in passenger traffic, serving 8,476,172 passengers in 2009/10 with a 7.4% increase.

**QA pair 3 : Easy # 1-2, 2-1, 2-2**

    Q.  Summarize the basic information of the airports located in Alaska and Virginia in the list of the busiest airports in the United States.
    A.  The airports located in Alaska and Virginia include Ted Stevens Anchorage International Airport in Anchorage, Alaska, and Washington Dulles International Airport in Dulles, Virginia. Ted Stevens Anchorage International Airport (ANC) is ranked 2nd among the busiest cargo throughput airports in the United States, handling 2,543,105 tonnes of cargo in 2010/11, with a 3.9% increase from the previous year. Washington Dulles International Airport (IAD) is ranked 9th in terms of passenger traffic, serving 6,167,501 passengers in 2009/10, with a 9.5% increase from the previous year.

**QA pair 4 : Easy # 1-3**

    Q.  Considering both cargo throughput and passenger traffic, which airport may be the largest in the list of the busiest airports in the United States, and why?
    A.  The largest airport, considering both cargo throughput and passenger traffic, may be John F. Kennedy International Airport (JFK) in Queens, New York. In terms of passenger traffic, John F. Kennedy International Airport is ranked 1st in the United States, handling 22,702,882 passengers in 2009/10, with a 5.8% increase from the previous year. For cargo throughput, JFK is ranked 6th, managing 1,348,992 tonnes of cargo in 2010/11, with a 0.5% increase. This strong performance in both passenger and cargo handling suggests that JFK could be considered the largest airport overall.

**QA pair 5 : Hard**

    Q.  Summarize the basic information of the airports that appear in both cargo throughput ranking and passenger traffic ranking about the busiest airports in the United States.
    A.  The airports that appear in both tables are John F. Kennedy International Airport, Miami International Airport, Los Angeles International Airport, Newark Liberty International Airport, and O’Hare International Airport. John F. Kennedy International Airport (JFK) is located in Queens, New York. It is ranked 6th in terms of cargo handled, with 1,348,992 tonnes in 2010/11 and a 0.5% increase. For passenger traffic, it is ranked 1st, handling 22,702,882 passengers in 2009/10 with a 5.8% increase. Miami International Airport (MIA) is located in Miami, Florida. It is ranked 4th for cargo, handling 1,841,929 tonnes in 2010/11 with a 0.3% increase. For passenger traffic, it is ranked 2nd, with 16,207,353 passengers in 2009/10, showing a 5.3% increase. Los Angeles International Airport (LAX) is located in Los Angeles, California. It is ranked 5th for cargo, handling 1,681,611 tonnes in 2010/11 with a 3.8% increase. For passengers, it is ranked 3rd, with 15,505,566 passengers in 2009/10, showing a 4.7% increase. Newark Liberty International Airport (EWR) is located in Newark, New Jersey. It is ranked 9th for cargo, handling 813,209 tonnes in 2010/11 with a 5.0% increase. For passengers, it is ranked 4th, handling 11,372,186 passengers in 2009/10, with a 6.6% increase. O’Hare International Airport (ORD) is located in Chicago, Illinois. It is ranked 7th for cargo, handling 1,311,622 tonnes in 2010/11 with a 4.7% increase. For passengers, it is ranked 5th, with 10,426,287 passengers in 2009/10, showing a 2.5% increase.
