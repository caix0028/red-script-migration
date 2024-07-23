#STK scheduled report
library(xlsx)
library(readr)
library(dplyr)
library(tidyr)

setwd("/home/docker/R_Workspace/YH_RScripts_wd/02 Scheduled Tasks/STK energy monthly report")
source('/home/docker/R_Workspace/monitoring/YH_RScripts/zz Not Run/User defined functions/yh_udf_latest.R')

#date select previous month
msg0<-paste("Selecting date range for last month: ","(",format(rollback(Sys.Date()),"%B %Y"),")",sep="")
cat(msg0)
cat("\n")
last_day_prev_mth<-date(rollback(Sys.Date()))
date_start<-floor_date(last_day_prev_mth,unit="month")
date_end<-last_day_prev_mth
f.date_select(date.start = date_start,date.end = date_end)

#create new folder
f.folder_creator(format(date_start,"%Y%m%b"),append_timestamp = T,set_folder_wd = T)

#grab all STK meters using combined meter table with site, proj, cust, STK is cust id 31
STK_comb_meter_table<-f.get_table("select address,meter_name,excel_name,meter_id,meter_type from meter left join site using (site_id) left join proj using (proj_id) left join customer using (cust_id) left join meter_billing using (meter_id) where cust_id = 31 and billing=1")
STK_meter_ids<-STK_comb_meter_table$meter_id
STK_meter_ids_str<-paste(STK_meter_ids,collapse = ", ")

#grab comb energy table
comb_energy_table<-f.get_table(sprintf("select meter_id,date,energy from energy_patched where meter_id in (%s) and date >= '%s' and date <= '%s'",STK_meter_ids_str,date_start,date_end))

#create df to store number of days of data each meter has
data_availability_df<-data.frame(matrix(nrow=nrow(STK_comb_meter_table),ncol=2))
colnames(data_availability_df)<-c("meter_name","data_points")

for (i in 1:length(STK_meter_ids)){
  #populate data_availability_df
  data_points<-filter(comb_energy_table,meter_id == STK_meter_ids[i])%>%nrow()
  meter_name<-STK_comb_meter_table%>%filter(meter_id==STK_meter_ids[i])%>%select(meter_name)%>%unlist()
  data_availability_df$meter_name[i]<-meter_name
  data_availability_df$data_points[i]<-data_points
  #write log if any lostcomms found
  if(data_points<no_of_days){
    msg1<-sprintf("%s only has %s data points",meter_name,data_points)
    write(msg1,file="lostcomms_detected_log.log",append=T)
  }
}

#write data_availability_df as csv
write.csv(data_availability_df,"data_availability.csv")

comb_df=left_join(STK_comb_meter_table,comb_energy_table,by=c("meter_id"="meter_id"))%>%filter(!is.na(energy))
comb_df$date=as.Date(comb_df$date,c("%Y-%m-%d"))
comb_df=comb_df[order(comb_df$date),]

summary_df=comb_df%>%pivot_wider(names_from=meter_type,id_cols=address,values_from=energy,values_fn=list(energy=sum))
summary_df=as.data.frame(summary_df)
summary_df=mutate(summary_df,consumption=`Solar Generation`-`Solar Export`)
names(summary_df)=c("Site", "Generation(kWh)","Export(kWh)","Consumption(kWh)")

chinbee_final_df=comb_df%>%filter(address=="15 Chin Bee Drive")%>%select(c("excel_name","date","energy"))%>%pivot_wider(names_from=excel_name,values_from=energy)%>%as.data.frame
tuas16_final_df=comb_df%>%filter(address=="16 Tuas Ave 7")%>%select(c("excel_name","date","energy"))%>%pivot_wider(names_from=excel_name,values_from=energy)%>%as.data.frame
jbl249_final_df=comb_df%>%filter(address=="249 Jalan Boon Lay")%>%select(c("excel_name","date","energy"))%>%pivot_wider(names_from=excel_name,values_from=energy)%>%as.data.frame
benoi16_final_df=comb_df%>%filter(address=="16 Benoi Crescent")%>%select(c("excel_name","date","energy"))%>%pivot_wider(names_from=excel_name,values_from=energy)%>%as.data.frame



#Bind into one xlsx file
filename <- sprintf("STK-%s-MonthlyReport-GwExport.xlsx",format(date_start,"%b%Y"))
write.xlsx(summary_df, file=filename, sheetName="Summary_Table", row.names=FALSE)
write.xlsx(chinbee_final_df, file=filename,sheetName="15_ChinBeeDrive",row.names=FALSE,append=TRUE)
write.xlsx(tuas16_final_df, file=filename,sheetName="16_TuasAve7",row.names=FALSE,append=TRUE)
write.xlsx(jbl249_final_df, file=filename,sheetName="249_JalanBoonLay",row.names=FALSE,append=TRUE)
write.xlsx(benoi16_final_df, file=filename,sheetName="16_BenoiCres",row.names=FALSE,append=TRUE)

#format the xlsx
wb <- loadWorkbook(filename)
sheets <- getSheets(wb)
autoSizeColumn(sheets[[1]], colIndex=1:ncol(summary_df))
autoSizeColumn(sheets[[2]], colIndex=1:ncol(chinbee_final_df))
autoSizeColumn(sheets[[3]], colIndex=1:ncol(tuas16_final_df))
autoSizeColumn(sheets[[4]], colIndex=1:ncol(jbl249_final_df))
autoSizeColumn(sheets[[5]], colIndex=1:ncol(benoi16_final_df))
saveWorkbook(wb,filename)
