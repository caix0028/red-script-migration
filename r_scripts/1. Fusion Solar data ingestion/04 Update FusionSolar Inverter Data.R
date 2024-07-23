library(xlsx)
library(jsonlite)
library(httr)
library(dplyr)
library(knitr)

#setwd("/home/docker/R_Workspace/YH_RScripts_wd/02 Scheduled Tasks/autorun monitoring file updater")
#autorun_wd=getwd()
#lost_comms_compile=read.xlsx(sprintf("%s/Lost comms compilation/Lost_comms_compile.xlsx",autorun_wd),sheetName = "lost_comms")
#login params - FusionSolar

inverter_state = read.csv("/home/docker/R_Workspace/YH_RScripts_wd/02 Scheduled Tasks/autorun monitoring file updater/fusionsolar_inverter_state.csv")
fs.baseRestURL = "https://sg5.fusionsolar.huawei.com"
fs.APIPath = "/thirdData/login"
fs.url_api = paste0(fs.baseRestURL,"/thirdData/getDevKpiDay")
fs.url_status_api=paste0(fs.baseRestURL,"/thirdData/getDevRealKpi")
fusionsolar_username = "SUNSEAP_API"
fusionsolar_password = "SUNSEAP123"
fs.completeRestURL=paste0(fs.baseRestURL,fs.APIPath)

#AzureSQL
fs_inv_list<-f.get_table("select inv_id,portal_ref as devIds,inv_name from inv
                                 where monitoring_portal='FusionSolar' and portal_ref is not null")
#fs_meter_list=rbind(fs_meter_list,c(as.integer(0000),"NE=33713512","DBS"))
inv_id_fs=paste0(fs_inv_list$inv_id,collapse=",")
#AzureSQL
t.proj_fs<-f.get_table("select inv_id,inv_name,meter_id,meter_name,meter_type,proj_name,proj_id,inv.monitoring_portal as monitoring_portal from inv
                    left join meter using (meter_id)
                    left join site using (site_id)
                    left join proj using (proj_id)")
#AzureSQL
t.fs_inv_energy<-f.get_table(sprintf("select * from inv_energy_patched where date >= '%s' and inv_id in (%s)",Sys.Date()-months(5),inv_id_fs))
fs_inv_energy_date<-as.Date(t.fs_inv_energy$date)
fs.date_start<-max(unique(fs_inv_energy_date))+1
fs.date_end<-Sys.Date()-1

update_fs_inv_energy_flag<-T
if(fs.date_start==Sys.Date()){
  print("FusionSolar inv energy data for yesterday already exists, not updating inv_energy_patched")
  update_fs_inv_energy_flag<-F
}


if(update_fs_inv_energy_flag){
  print(sprintf("updating inv_energy_patched table (FusionSolar) for %s to %s",fs.date_start,fs.date_end))
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
  
  
  print("FusionSolar - getting inv energy data from FusionSolar ...")
  devIds=paste0(fs_inv_list$devIds,collapse=",")
  inv_energy_portal=NULL
  
  for (i in querytime){
    retrycount=0
    get_data=NULL
    is_response=F
    api_success=F
    
    while((retrycount<=20)&((!is_response)|(!api_success))){
      get_data <- tryCatch({POST(fs.url_api,body=list(devIds = devIds,collectTime=i,devTypeId=1),add_headers(.headers = c("XSRF-TOKEN"=fs_token)),encode="json")}, error = function(k){print(k)})
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
    get_data_text = get_data_text%>%select(c("collectTime","dataItemMap.product_power","devId"))
    get_data_text$collectTime = as.POSIXct((get_data_text$collectTime)/1000,origin = "1970-01-01",tz="")
    names(get_data_text)=c("date","inv_energy","devIds")
    get_data_text$devIds=as.character(get_data_text$devIds)
    get_data_text=get_data_text %>% left_join(fs_inv_list,by=c("devIds"="devIds"))
    get_data_text$inv_energy=round(as.double(get_data_text$inv_energy),5)
    inv_energy_portal=rbind(inv_energy_portal,get_data_text[c("date","inv_id","inv_energy")])
    
  }
  inv_energy_portal$date=as.character(as.Date(inv_energy_portal$date,tz=""))
  fs.inv_energy_to_patch=inv_energy_portal%>%filter(date %in% date_vec)
  
  if (nrow(fs.inv_energy_to_patch)>0){
    #append to database
    #AzureSQL
    sql_write_done<-f.append_to_table(fs.inv_energy_to_patch,dbname = "monitoring",tablename="inv_energy_patched")
    if(sql_write_done){
      print("FusionSolar - data appended to table")
    }else{
      print("FusionSolar - append failed")
    }
  }else{print("FusionSolar - No data appended")}
  
  
  #get plant status
  print("FusionSolar - getting device status ...")
  retrycount=0
  get_status=NULL
  is_response=F
  api_success=F
  
  while((retrycount<=20)&((!is_response)|(!api_success))){
    get_status <- tryCatch({POST(fs.url_status_api,body=list(devIds = devIds,devTypeId=1),add_headers(.headers = c("XSRF-TOKEN"=fs_token)),encode="json")}, error = function(k){print(k)})
    retrycount=retrycount+1
    if(class(get_status)=="response"){
      is_response=T
      api_status=content(get_status,"parsed")
      if(api_status$success==T){
        api_success=T
        print("success")
      }else{
        print(sprintf("failcode: %s - message - %s",api_status$failCode, api_status$message))
      }
    }
  }
  
  get_status_text <- content(get_status, "text") %>% fromJSON(flatten = TRUE)
  get_status_text = get_status_text$data
  connectivity = get_status_text%>%select(c("devId","dataItemMap.run_state"))
  names(connectivity)=c("devIds","status")
  connectivity$devIds=as.character(connectivity$devIds)
  connectivity = connectivity %>% left_join(fs_inv_list,by=c("devIds"="devIds"))
  connectivity$date=Sys.Date()
  connectivity = connectivity[order(connectivity$inv_id),]
  
  connectivity = connectivity %>% left_join(inverter_state,by=c("status"="inverter_state"))

  connectivity$inv_id=as.integer(connectivity$inv_id)
  lost_comm_fs_inv=filter(connectivity,!is.na(Description))%>%left_join(t.proj_fs,by=c("inv_id"="inv_id","inv_name"="inv_name"))%>%select(c("date","inv_name","inv_id","proj_name","proj_id","Description","monitoring_portal"))
  no_fs_inv_lost=nrow(lost_comm_fs_inv)
  
  if(no_fs_inv_lost>0){
    print(kable(lost_comm_fs_inv))
  }
  
  
}


