# Shot 3 : Open-WikiTable

## NL query

    what school did the player who played in 2004-05 attend among the toronto raptors all-time w roster?

## SQL query

    SELECT School_Club_Team FROM table_1_10015132_21 WHERE Years_in_Toronto = '2004-05';

## SQL query result (serialization)

    [row 1]: Xavier (Ohio)

## Statement

    The player from the Toronto Raptors’ all-time "W" roster who played in the 2004-05 season attended Xavier University (Ohio).
