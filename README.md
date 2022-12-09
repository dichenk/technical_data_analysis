The project implements search, sorting and caching algorithms for working with a large amount of data (an archive for 5 years) based on stock quotes of companies from the S&P 500.

- The `pandas` library is used to read the data.
- Quick sorting method *Quick Sort* is used to sort quotes, company tickers and dates.
- The search for data requested by the client is carried out on sorted data through a binary search algorithm.
- A caching mechanism has been implemented to save computing resources if the request from the user is repeated.

The project is designed as a poetry package with scripts that allow the user to get a sorted selection of data and search for data
