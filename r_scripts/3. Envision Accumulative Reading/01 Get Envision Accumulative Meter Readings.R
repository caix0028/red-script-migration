source('/home/docker/R_Workspace/monitoring/YH_RScripts/zz Not Run/User defined functions/yh_udf_latest.R') #yhudf

library(httr)
library(jsonlite)
library(xlsx)

wd<-"/home/docker/R_Workspace/YH_RScripts_wd/02 Scheduled Tasks/Get Envision Readings"
setwd(wd)

initwd<-f.folder_creator("Envision_meter_readings",set_folder_wd = T)

token=tryCatch({token}, error = function(k){})
if(is.null(token)){
  source('/home/docker/R_Workspace/monitoring/YH_RScripts/08 Daily Autorun - breakdown/02a Get Envision API Token.R')
}

access_key="79aba484-082d-4863-8a71-2a9ed11c2786"
secret_key="c3e6a1ad-7a07-4711-a47c-81b42c01a01d"
orgId="o16221928963871049"

baseRestURL = "https://app-portal-eu2.envisioniot.com"
statusPath = "/solar-api/domainService/getmdmidspoints"
url_status_api=paste0(baseRestURL,statusPath)
envision_meter_list<-f.get_table("select meter_id,api_metrics,portal_ref as mdmids,meter_name from meter
                                 where monitoring_portal='Envision' and portal_ref is not null and portal_ref!='Inverter'")
t.proj<-f.get_table("select meter_id,meter_name,meter_type,proj_name,proj_id,monitoring_portal from meter
                    left join site using (site_id)
                    left join proj using (proj_id)")


api_metric=unique(envision_meter_list$api_metrics)
api_metric=api_metric[!is.na(api_metric)]
meter_readings=data.frame()
for (i in 1:length(api_metric)){
  metrics=api_metric[i]
  env_meter_list_temp=envision_meter_list[which(envision_meter_list$api_metrics==metrics),]
  
  mdmids_length=length(env_meter_list_temp$mdmids)
  slice_num=50
  round=ceiling(mdmids_length/slice_num)
  
  for (j in 1:round){
    if (j<round){
      mdmids=paste0(env_meter_list_temp$mdmids[((slice_num*(j-1)+1):(slice_num*j))],collapse=",")
    }else if (j==round){
      mdmids=paste0(env_meter_list_temp$mdmids[(slice_num*(j-1)+1):mdmids_length],collapse=",")
    }
  #mdmids=paste0(envision_meter_list$mdmids[which(envision_meter_list$api_metrics==metrics)],collapse=",")
  print("Envision - getting meter connectivity status ...")
  retrycount=0
  get_status=NULL
  is_response=F
  api_success=F
  
  device=substr(metrics,1,3)
  
  while((retrycount<=20)&((!is_response)|(!api_success))){
    get_status <- tryCatch({GET(url_status_api,query=list(mdmids=mdmids,points=paste0(device,".APProductionKWH,",device,".APConsumedKWH"),`Access Key`=access_key,`Secret Key`=secret_key,orgId=orgId,field="value"))}, error = function(k){print(k)})
    retrycount=retrycount+1
    if(class(get_status)=="response"){
      is_response=T
      api_status=content(get_status,"parsed")
      if(api_status$status==0){
        api_success=T
        print("success")
      }else{
        print(sprintf("error: %s - %s - %s",api_status$status, api_status$msg, api_status$submsg))
      }
    }
  }
  
  get_status_text <- content(get_status, "parsed")$result
  get_status_text=sapply(get_status_text,unlist)%>%t()%>%as.data.frame()
  mdmids=rownames(get_status_text)
  get_status_text=get_status_text%>%select(c(paste0("points.",device,".APProductionKWH.timestamp"),paste0("points.",device,".APProductionKWH.value"),paste0("points.",device,".APConsumedKWH.value")))
  get_status_text=sapply(get_status_text,as.numeric)%>%as.data.frame()
  if(nrow(get_status_text)==3&&ncol(get_status_text)==1){
    get_status_text=t(get_status_text)%>%as.data.frame()}
  names(get_status_text)=c("timestamp","exported","imported")
  rownames(get_status_text)=mdmids
  get_status_text=mutate(get_status_text,mdmids=rownames(get_status_text))
  get_status_text$timestamp=as.POSIXct(get_status_text$timestamp/1000, origin="1970-01-01")
  get_status_text = get_status_text %>% left_join(env_meter_list_temp[which(env_meter_list_temp$api_metrics==metrics),],by=c("mdmids"="mdmids"))
  get_status_text = get_status_text[order(get_status_text$meter_id),]
  get_status_text= get_status_text%>%mutate(lost_comms=date(get_status_text$timestamp)<date(Sys.time())|abs(hour(Sys.time())-hour(get_status_text$timestamp))>1)
  meter_readings=rbind(meter_readings,get_status_text)
  }
}



filename=paste0("Envision_meter_readings",format(Sys.time(),"_%Y%m%d_%H%M%S"),".csv")
write.csv(meter_readings,filename,row.names = F)

