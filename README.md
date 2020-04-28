# local-stock-data-maintainer
maintain stock data read remote source, e.g. sina
in order to use data in a fast way & reduce the burden of remote server

落地外部源（比如sina）股票&期货数据，以便于本地快速使用（并减轻源服务器负担）

version 0.0.1
- retrieving daily K lines of futures contracts from sina.com.cn
- retrieving stock data by tushare lib.
- a simple sqlite db manager for managing local data
