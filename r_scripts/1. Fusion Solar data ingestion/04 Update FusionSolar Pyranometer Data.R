library(xlsx)
library(jsonlite)
library(httr)
library(dplyr)
library(knitr)

#setwd("/home/docker/R_Workspace/YH_RScripts_wd/02 Scheduled Tasks/autorun monitoring file updater")
#autorun_wd=getwd()
#lost_comms_compile=read.xlsx(sprintf("%s/Lost comms compilation/Lost_comms_compile.xlsx",autorun_wd),sheetName = "lost_comms")
#login params - FusionSolar
fs.baseRestURL = "https://sg5.fusionsolar.huawei.com"
fs.APIPath = "/thirdData/login"
fs.url_api = paste0(fs.baseRestURL,"/thirdData/getKpiStationDay")
#fs.url_status_api=paste0(fs.baseRestURL,"/thirdData/getDevRealKpi")
fusionsolar_username = "SUNSEAP_API"
fusionsolar_password = "SUNSEAP123"
fs.completeRestURL=paste0(fs.baseRestURL,fs.APIPath)

#AzureSQL
fs_pyr_list<-f.get_table("select pyr_id,portal_ref as stationcodes,location from pyr
                                 where monitoring_portal='FusionSolar' and portal_ref is not null")
#fs_meter_list=rbind(fs_meter_list,c(as.integer(0000),"NE=33713512","DBS"))
pyr_id_fs=paste0(fs_pyr_list$pyr_id,collapse=",")
#AzureSQL
t.fs_sunhours<-f.get_table(sprintf("select * from pyr_SH where date >= '%s' and pyr_id in (%s)",Sys.Date()-months(5),pyr_id_fs))
fs_sunhours_date<-as.Date(t.fs_sunhours$date)
fs.date_start<-max(unique(fs_sunhours_date))+1
fs.date_end<-Sys.Date()-1

update_fs_sunhours_flag<-T
if(fs.date_start==Sys.Date()){
  print("FusionSolar sunhours data for yesterday already exists, not updating pyr_SH")
  update_fs_sunhours_flag<-F
}


if(update_fs_sunhours_flag){
  print(sprintf("updating pyr_SH table (FusionSolar) for %s to %s",fs.date_start,fs.date_end))
  no_of_days<-as.numeric(difftime(as.Date(fs.date_end),as.Date(fs.date_start)),units="days")+1
  
  date_vec<-character()
  for(i in 0:no_of_days-1){
    date_vec[i+1]<-as.character(as.Date(fs.date_start)+i)
  }
  
  print("FusionSolar - getting api token ...")
  httr::set_config(config(ssl_verifypeer = 0L))
  retrycount=0
  response=NULL
  is_response=F
  api_success=F
  while((retrycount<=20)&((!is_response)|(!api_success))){
    response <- tryCatch({POST(fs.completeRestURL, body = list(userName = fusionsolar_username, systemCode = fusionsolar_password), encode="json")}, error = function(k){print(k)})
    retrycount=retrycount+1
    if(class(response)=="response"){
      is_response=T
      api_status=content(response,"parsed")
      if(api_status$success==T){
        api_success=T
        print("success")
      }else{
        print(sprintf("failcode: %s - message - %s",api_status$failCode, api_status$message))
      }
    }
  }
  response_text <- headers(response)
  fs_token=response_text$`xsrf-token`
  
  
  
  #get energy data
  month_diff=month(fs.date_end)-month(fs.date_start)
  
  if(month_diff==0){
    querytime=paste0(fs.date_start," 00:00:00")
    querytime=as.numeric(as.POSIXlt(querytime))*1000
  }
  
  if(month_diff>0){
    begin_time=as.Date(paste(year(fs.date_start),month(fs.date_start),"01",sep="-"))
    querytime=paste0(begin_time," 00:00:00")
    for (i in 1:month_diff){
      querytime=c(querytime,paste0(begin_time%m+% months(i)," 00:00:00"))
    }
    querytime=as.numeric(as.POSIXlt(querytime))*1000
  }
  
  
  print("FusionSolar - getting sunhours data from FusionSolar ...")
  stationcodes=paste0(fs_pyr_list$stationcodes,collapse=",")
  sunhours_portal=NULL
  
  for (i in querytime){
    retrycount=0
    get_data=NULL
    is_response=F
    api_success=F
    
    while((retrycount<=20)&((!is_response)|(!api_success))){
      get_data <- tryCatch({POST(fs.url_api,body=list(stationCodes = stationcodes,collectTime=i),add_headers(.headers = c("XSRF-TOKEN"=fs_token)),encode="json")}, error = function(k){print(k)})
      retrycount=retrycount+1
      if(class(get_data)=="response"){
        is_response=T
        api_status=content(get_data,"parsed")
        if(api_status$success==T){
          api_success=T
          print("success")
        }else{
          print(sprintf("failcode: %s - message - %s",api_status$failCode, api_status$message))
        }
      }
    }
    get_data_text <- content(get_data, "text") %>% fromJSON(flatten = TRUE)
    get_data_text = get_data_text$data
    get_data_text = get_data_text%>%select(c("collectTime","dataItemMap.radiation_intensity","stationCode"))
    get_data_text$collectTime = as.POSIXct((get_data_text$collectTime)/1000,origin = "1970-01-01",tz="")
    names(get_data_text)=c("date","sunhours","stationcodes")
    get_data_text=get_data_text %>% left_join(fs_pyr_list,by=c("stationcodes"="stationcodes"))
    get_data_text$sunhours=round(as.double(get_data_text$sunhours),5)
    sunhours_portal=rbind(sunhours_portal,get_data_text[c("date","pyr_id","sunhours")])
    
  }
  sunhours_portal$date=as.character(as.Date(sunhours_portal$date,tz=""))
  fs.sunhours_to_patch=sunhours_portal%>%filter(date %in% date_vec)
  if (nrow(fs.sunhours_to_patch)==0||filter(fs.sunhours_to_patch,date==max(date))$sunhours==0){
    fs_pyr_lostcomms=select(fs_pyr_list,c("pyr_id","location"))
    fs_pyr_lostcomms$date=Sys.Date()
    fs_pyr_lostcomms$status="Lost Comms"
    fs_pyr_lostcomms=select(fs_pyr_lostcomms,c("date","pyr_id","location","status"))
    cat("\n")
    cat("FUSIONSOLAR PYRANOMETER LOST COMMS\n")
    cat(sprintf("Fusionsolar pyranometers lost comms as of %s",Sys.Date()))
    print(kable(fs_pyr_lostcomms))
    cat("\n")
  }

  if (nrow(fs.sunhours_to_patch)>0){
    #append to database
    #AzureSQL
    sql_write_done<-f.append_to_table(fs.sunhours_to_patch,dbname = "monitoring",tablename="pyr_SH")
    if(sql_write_done){
      print("FusionSolar - data appended to table")
    }else{
      print("FusionSolar - append failed")
    }
  }else{print("FusionSolar - No data appended")}
  
  

}


