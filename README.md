# DB1B Market Data

This is a script to enrich DB1B market data.
It is compatible with DB1B market files with the following five columns:
- ORIGIN
- DEST
- TICKET_CARRIER
- PASSENGERS
- MARKET_FARE

These files are available for download, one quarter of data at a time, here: https://www.transtats.bts.gov/tables.asp?QO_VQ=EFI&QO_anzr=Nv4yv0r

## About DB1B market data

DB1B data is a 10% sample of air tickets sold by carriers that report data to the Bureau of Transportation Statistics.
It is not specified how the 10% sample is taken.  To put a finer point on it, I don't have proof that airlines are not selecting the 10% of tickets least competitvely disadvantageous to disclose to their competition.  But, I also have not noticed any data smells indicating gamesmanship may be afoot.
It is also not specified what goes into reporting fares.  From looking at the data, I suspect ancillary items are excluded from the fares reported.
