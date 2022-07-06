# 1. Wczytaj plik autoSmall.csv i wypisz pierwsze 5 wierszy

data <- read.csv("autaSmall.csv", encoding = "UTF-8")
head(data, 5)
nrow(data)

# 2. Pobierz dane pogodowe z REST API
install.packages("httr")
install.packages("jsonlite")

library(httr)
library(jsonlite)

endpoint <- "https://api.openweathermap.org/data/2.5/weather?q=Warszawa&appid=1765994b51ed366c506d5dc0d0b07b77"

getWeather <- GET(endpoint)
weatherText <- content(getWeather,"text")
View(weatherText)
weatherJSON<-fromJSON(weatherText)
wdf<- as.data.frame(weatherJSON)
View(wdf)

# 3. Napisz funkcję zapisującą porcjami danych plik csv do tabeli w SQLite
#Utworzenie bazy na podstawie pliku auta2.csv - 3.2GB

install.packages("DBI")
install.packages("RSQLite")
library(DBI)
library(RSQLite)

con <- dbConnect(SQLite(), "auta2.sqlite")

readToBase<-function(filepath,con,tablename,size=100, sep=",",header=TRUE,delete=TRUE, encoding="UTF-8"){
  ap = !delete
  ov = delete
  
  fileCon <- file(description=filepath, open = "r", encoding = encoding)
  
  df1 <- read.table(fileCon, header = TRUE, sep=sep, fill=TRUE,
                    fileEncoding = encoding, nrows = size)
  if( nrow(df1)==0)
    return(0)
  myColNames <- names(df1)
  dbWriteTable(con, tablename, df1, append=ap, overwrite=ov)
  # zapis do bazy
  repeat{
    if(nrow(df1)==0){
      close(fileCon)
      dbDisconnect(con)
      break;
    }
    df1 <- read.table(fileCon, col.names = myColNames, sep=sep,
                      fileEncoding = encoding, nrows = size)
    dbWriteTable(con, tablename, df1, append=TRUE, overwrite=FALSE)
  }
}

readToBase("auta2.csv", con, "auta2", 1000)


#4.Napisz funkcję znajdującą tydzień obserwacji z największą średnią ceną ofert korzystając z zapytania SQL.

library(DBI)
library(RSQLite)

con <- dbConnect(SQLite(), "auta2.sqlite")
query <- "SELECT tydzien, avg_week_price 
          FROM 
          (
            SELECT tydzien, AVG(cena) as avg_week_price 
            FROM auta2
            GROUP BY tydzien
          ) 
          WHERE avg_week_price=(SELECT  max(avg_week_price) 
                        FROM (select tydzien, AVG(cena) as avg_week_price 
                              FROM auta2 GROUP BY tydzien))"
max_avg_week_price <- dbSendQuery(con, query)
result <- dbFetch(max_avg_week_price)
print(result)
dbClearResult(res)
dbDisconnect(con)


#5 Podobnie jak w poprzednim zadaniu napisz funkcję znajdującą tydzień 
#  obserwacji z największą średnią ceną ofert  tym razem wykorzystując REST api.

library(httr)
library(jsonlite)

url <- "http://54.37.136.190:8000/week?t="
weeks_avg_price_df = NULL
i = 0
repeat
{
  i <- i + 1 
  page <- i
  week_url <- paste(url, page, sep="")
  getWeek <- GET(week_url)
  getWeek_text <- content(getWeek, "text")
  getWeek_json <- fromJSON(getWeek_text, flatten = TRUE)
  getWeek_df <- as.data.frame(getWeek_json)
  getWeek_avg_price <- mean(getWeek_df$cena, na.rm = TRUE)
  print(getWeek_avg_price)
  if(getWeek_avg_price == 0) {
    break;
  }
  weeks_avg_price_df = rbind(weeks_avg_price_df, data.frame(page, getWeek_avg_price))
}

getWeek_max_avg_price <- subset(weeks_avg_price_df,weeks_avg_price_df$getWeek_avg_price == max(weeks_avg_price_df$getWeek_avg_price))
View(getWeek_max_avg_price)

write.csv(weeks_avg_price_df,"weeks_avg_price_df.csv", row.names = FALSE)
