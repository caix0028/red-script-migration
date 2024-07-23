library(dplyr)
library(formattable)
library(tidyr)
library(xlsx)

# Setting the working directory
setwd("/home/docker/R_Workspace/YH_RScripts_wd/02 Scheduled Tasks/Corp dev report generator")
source('/home/docker/R_Workspace/monitoring/YH_RScripts/zz Not Run/User defined functions/yh_udf_latest.R')

#if manual run with r_menu
#wd<-paste0(userdata$wd,"/02 Scheduled Tasks/Report for corp dev")
#setwd(wd)

date_end<-date(rollback(Sys.Date()))
exclusion_df<-read.csv("input.txt",comment.char = "#")
excluded_projs_str<-exclusion_df$input_df[1]
excluded_meters_str<-exclusion_df$input_df[2]

energy_gen_query<-sprintf("
Select p.proj_id, p.proj_name,s.date_of_com as turn_on_date,year(ep.date) as year, month(ep.date) as month, sum(ep.energy) as energy_generated from energy_patched as ep
Left join meter as m
Using (meter_id)
Left join site as s
Using (site_id)
Left join proj as p
Using (proj_id)
Where p.proj_id not in (%s)
And m.meter_id not in (%s)
And (ep.date>= date_of_com and ep.date <= '%s')
And m.meter_type = 'Solar Generation'
And s.country='Singapore'
Group by p.proj_id, m.meter_type, year(ep.date), month(ep.date);",excluded_projs_str,excluded_meters_str,date_end)

energy_ex_query<-sprintf("
Select p.proj_id, p.proj_name, year(ep.date) as year, month(ep.date) as month, sum(ep.energy) as energy_exported from energy_patched as ep
Left join meter as m
Using (meter_id)
Left join site as s
Using (site_id)
Left join proj as p
Using (proj_id)
Where p.proj_id not in (%s)
And m.meter_id not in (%s)
And (ep.date>= date_of_com and ep.date <= '%s')
And m.meter_type = 'Solar Export'
And s.country='Singapore'
Group by p.proj_id, m.meter_type, year(ep.date), month(ep.date);",excluded_projs_str,excluded_meters_str,date_end)

system_size_query<-sprintf("
SELECT p.proj_id, p.proj_name, ss.year, sum(ss.system_size) as system_size FROM system_size as ss
Left join meter as m
Using (meter_id)
Left join site as s
Using (site_id)
Left join proj as p
Using (proj_id)
Where p.proj_id not in (%s)
And m.meter_id not in (%s)
And meter_type = 'Solar Generation'
And s.country='Singapore'
Group by p.proj_id, ss.year",excluded_projs_str,excluded_meters_str)

pyr_SH_query<-sprintf("
Select p.proj_id, p.proj_name, year(pyr_SH.date) as year, month(pyr_SH.date) as month, sum(pyr_SH.sunhours)/count(distinct s.site_id) as sunhours from pyr_SH
Left join site as s
Using (pyr_id)
Left join proj as p
Using (proj_id)

Where p.proj_id not in (%s)

And (pyr_SH.date >= date_of_com and pyr_SH.date <= '%s')
Group by p.proj_id, year(pyr_SH.date), month(pyr_SH.date)",excluded_projs_str,date_end)


# Loading the csv files into data frames
df_EnergyGen <- f.get_table(energy_gen_query)
df_EnergyExp <- f.get_table(energy_ex_query)
df_SysSize <- f.get_table(system_size_query)
df_Sunhours <- f.get_table(pyr_SH_query)

# Combining the dataframes together using left joins
df_EnergyGenExp <- left_join(df_EnergyGen, df_EnergyExp, by = c("proj_id","proj_name", "year", "month"))
df_Energy_SS <- left_join(df_EnergyGenExp, df_SysSize, by = c("proj_id", "proj_name","year"))
df_Energy_SS_SH <- left_join(df_Energy_SS, df_Sunhours, by = c("proj_id","proj_name", "year", "month"))

# Remove records in the year 2012, as there isn't any sunhours information in 2012
# Create three new columns - Export ratio, Yield, Performance Ratio
df_final <- df_Energy_SS_SH %>% 
  filter(year != 2012) %>%
  mutate(export_ratio = energy_exported/energy_generated) %>%
  mutate(yield = energy_generated/system_size) %>%
  mutate(performance_ratio = yield/sunhours)

df_final[,"export_ratio"] <- percent(df_final[,"export_ratio"])
df_final[,"performance_ratio"] <- percent(df_final[,"performance_ratio"])

df_final[which(df_final$proj_id %in% c(1,7,31)),"turn_on_date"]="-"#HDB P1,2,3
df_final=df_final[order(df_final$turn_on_date,df_final$proj_id,df_final$year,df_final$month),]
# Exporting result into an Excel file in a new folder
f.folder_creator(f_name = sprintf("Corpdev_report_%s",format(Sys.Date(),"%Y%m%b")),set_folder_wd = T)
write.xlsx(df_final, sprintf("Sunseap Leasing Solar Portfolio till %s.xlsx",format(date_end,"%B %Y")), row.names = FALSE)
