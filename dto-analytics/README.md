This is the application that contains all the dashboards, scheduled reports, lookups and other supports for the DTO agency and public dashboards.

Requirements: getwatchlist addon
Generally, this is not useful unless you have the GABQ addon as well, because you need to have data to work with.

The govauRegistrants.csv lookup is only useful if you're looking at hosts in the .gov.au namespace, however if you want to examine other namespaces you could replace it with another lookup. The data needs to be in the same form; that is, a CSV with two columns, domain and registrant. It must include a header line with the column names. You will need to update the "Update govauRegistrants" search to point the URL at your source. It should be scheduled to update periodically.

