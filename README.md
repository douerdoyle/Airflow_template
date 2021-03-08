# 說明
程式碼有內有分 SourceCode 與 Airflow 資料夾<br>
SourceCode 是放可以直接執行的 Python 程式<br>
Airflow 則是放需透過 docker-compose 建立的 Airflow workflow<br>
## 程式執行流程概述
本程式會去下載 106 年第一季至 109 年第四季的台北市、新北市、高雄市的不動產公開資料以及桃園市、台中市的預售屋資料，下載完成後會將資料合併、篩選並輸出資料<br>
程式會篩選：

1. 「主要用途」為「住家用」<br>
2. 「建物型態」為「住宅大樓」<br>
3. 「總樓層數」大於等於十三樓<br>

輸出為 filter.csv<br>

並且會統計

1. 總件數
2. 總車位數
3. 平均總價元
4. 平均車位總價元

輸出為 count.csv<br>

最後會合併 filter.csv 與 count.csv 為 result.csv<br>

## 可直接執行的 Python 程式說明
### 資料夾說明
./SourceCode/：放置程式檔案的資料夾<br>
./SourceCode/DATA/：內政部不動產實價登錄 Open Data CSV 檔下載後儲存處<br>
./SourceCode/OUTPUT/：程式處理後輸出的資料 CSV 檔放置處<br>
### 檔案說明
./SourceCode/func_tools.py：執行程式時所需要使用的 Function 放置處<br>
./SourceCode/main.py：主要執行的程式<br>
./SourceCode/OUTPUT/filter.csv：第一個篩選之後輸出的檔案<br>
./SourceCode/OUTPUT/count.csv：統計之後輸出的檔案<br>
./SourceCode/OUTPUT/result.csv：第二個篩選之後輸出的檔案<br>

### 執行流程說明
1. 請確認已安裝 Python 且版本為 3.7 或以上
2. 進入 ./SourceCode/ 這個資料夾<br>
3. 確認 ./SourceCode/DATA/ 與 ./SourceCode/OUTPUT/ 兩個資料夾是空的<br>
4. 輸入「python main.py」 執行程式<br>
5. 程式會開始下載 Open Data 至 ./SourceCode/DATA/ 這個資料夾中
6. 程式會讀取 ./SourceCode/Data/ 內，名稱包含「_lvr_land_」且副檔名為 csv 的資料
7. 程式處理資料，並將處理後的資料輸出至 ./SourceCode/OUTPUT/ 中，共會輸出 filter.csv、count.csv 以及 result.csv 三個檔案
8. 程式結束

## 透過 Airflow Docker Container 執行的 Python 程式說明
### 資料夾說明
./Airflow/：放置建置具備 Airflow 的 Docker Container 相關檔案與程式碼的資料夾<br>
./Airflow/image/：放置建置 Docker Image 所需的檔案<br>
./Airflow/src/：放置與 Container Volumes 映射的資料夾<br>
./Airflow/src/dags/：Dag 程式碼放置處<br>
./Airflow/src/OPENDATA/：內政部不動產實價登錄 Open Data CSV 檔下載後儲存處<br>
./Airflow/src/OUTPUT/：Dag 程式處理後輸出的資料 CSV 檔放置處<br>
./Airflow/lib/：放置 Dag 程式碼會需要使用的函式程式碼
### 檔案說明
./docker-compose.py：建立 postgres 資料庫與 Airflow Webserver 兩個 Container 的docker-compose 檔<br>
./Airflow/dags/cathay_quiz.py：Dag 程式碼，具備下載 Open Data、處理與輸出等功能<br>
./Airflow/lib/func_tools.py：執行程式時所需要使用的 Function 放置處<br>
./Airflow/OUTPUT/filter.csv：第一個篩選之後輸出的檔案<br>
./Airflow/OUTPUT/count.csv：統計之後輸出的檔案<br>
./Airflow/OUTPUT/result.csv：第二個篩選之後輸出的檔案<br>


### 執行流程說明
1. 請確定已安裝 Docker，類 UNIX 作業系統請額外確認是否已安裝 docker-compose 套件<br>
2. 進入 ./Airflow/ 這個資料夾<br>
3. 輸入 docker-compose up -d，建立 postgres 與 Airflow Container
4. 等 docker-compose 完成，開啟瀏覽器（建議使用 Chrome），在網址列輸入「[http://localhost:8080/](http://localhost:8080/)」並按下 Enter 進入該網址
5. 由於設定只會執行一次，所以進入網頁時，就會看到該程式已經處於執行（在 Container 建立時就會啟動程式）![Airflow 主頁畫面－程式正在執行](https://i.imgur.com/QaXaWkb.png)
6. 執行完成後，Recent Tasks 會出現以綠色圓圈包覆數字 4 的狀態，且 DAG Runs 下會出現以綠色圓圈包覆數字 1 的狀態![Airflow 主頁畫面－程式執行完成](https://i.imgur.com/CmMdfnl.png)
7. 程式執行完成後，會將處理後的資料輸出至 ./Airflow/OUTPUT/ 中，共會輸出 filter.csv、count.csv 以及 result.csv 三個檔案