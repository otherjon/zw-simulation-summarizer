#code for analyzing runs which track outputs every year

rm(list = ls())
setwd("/home/mves/Dropbox/Professional/NSF_SEES_Fellowship/ABM_results/Jun29_parametersweep")

#-------------------------
#    ASSEMBLE ALL DATA
#-------------------------

# create 'ModelList.txt' using, in a Linux terminal, "ls *dat > ModelList.txt" in the working directory
output.files<-read.csv("ModelList.txt",header=FALSE)
# make a list and pull each of the behaviorspace results into it, skipping 6 lines for the header
outfiles<-list()
for (i in 1:nrow(output.files))
	outfiles[[as.character(output.files[i,]) ]]<-read.csv(as.character(output.files[i,]),skip=6)
outfiles<-outfiles[!(names(outfiles) %in% c("RunSoftwareTests.dat"))] # remove software tests

#check that you have all runs you expect and they have the same number of columns
for (i in 1:length(outfiles))
	print(nrow(outfiles[[i]])) 
	# this one is a combination of years-run and runs-in-behaviorspace
for (i in 1:length(outfiles))
	print(length(outfiles[[i]])) 
	# currently should be 50 each
# create a unique ID for the behaviorspace name and the run within that behaviorspace
for (i in 1:length(outfiles))
	outfiles[[i]]$bsname<-rep(names(outfiles[i]),nrow(outfiles[[i]]))

# compile them into one table/data object
d<-outfiles[[1]]
for (i in 2:length(outfiles))
	d<-rbind(d,outfiles[[i]])

d$bs.run<-as.factor(paste(as.numeric(as.factor(d$bsname)),d$X.run.number.,sep="-"))

write.csv(d,"AllDataFullRun_2018-06-29.csv")
#this dataset includes runs with basically no thresholds except zero cows
#  and includes data for every year for these runs

#-------------------------------------
# PULL OUT ONLY LAST YEAR OF EACH RUN
#-------------------------------------

findEndYear<-function(ofile){
#find out which year is max for each run
ofile$run<-ofile$X.run.number.
end.year<-data.frame(run=names(tapply(ofile$X.step., ofile$run, max)),
	endyear=tapply(ofile$X.step., ofile$run, max))
ofile<-merge(ofile,end.year,by.x="run",by.y="run")
#make a data frame that just has the last row, last step, of each model
return(ofile[ofile$X.step==ofile$endyear,])
}

dfin.list<-lapply(outfiles,findEndYear)

#We do this for each results file separately because we get wall times for
#  each behaviorspace, so we want run times for that entire behaviorspace 
#  in order to calculate overhead
for (i in 1:length(dfin.list))
	print(nrow(dfin.list[[i]])) # should be the number of combos in each behaviorspace
for (i in 1:length(dfin.list))
	print(length(dfin.list[[i]])) # you've added a 'run', 'bsname', and 'endyear' column

#print out the run time information for each behaviorspace
for (i in 1:length(dfin.list)){
	print(names(dfin.list[i]))
	print((sum(dfin.list[[i]]$timer)+sum(dfin.list[[i]]$model.setup.time))/60)
	print(sd(dfin.list[[i]]$model.setup.time))
	print(sd(dfin.list[[i]]$timer))
}

# compile them into one table/data object
dfin<-dfin.list[[1]]
for (i in 2:length(dfin.list))
	dfin<-rbind(dfin,dfin.list[[i]])

# add a total internal time variable, a burn-in variable, and a sustainability variable
#   (these variables only make sense for the last year of a run)
dfin$total.internal.time<-dfin$model.setup.time+dfin$timer
hist(dfin$total.internal.time)
dfin$sustainable<-dfin$calendar.year==2011
dfin$burn.in<-dfin$calendar.year==0
dfin$bs.run<-as.factor(paste(as.numeric(as.factor(dfin$bsname)),dfin$run,sep="-"))

write.csv(dfin,"FullRun_2018-06-29.csv")
#this dataset includes runs with basically no thresholds except zero cows
#  and only the data for the final year

#------------------------------------------
# MERGE FINAL RESULTS INTO FINAL YEAR DATA
#------------------------------------------
#now work on merging in various summary variables from d into dfin 

# create a total.cows for the whole run (for calculating actual reproductive rate)
total.cows<-data.frame(bs.run=rownames(tapply(d$count.cows,d$bs.run,sum)),
	total.cows=as.numeric(tapply(d$count.cows,d$bs.run,sum)))
dfin<-merge(dfin,total.cows,by.x="bs.run",by.y="bs.run")
dfin$actual.reproductive.rate<- (dfin$total.number.of.births / (dfin$total.cows / (dfin$endyear ) ) )/ dfin$endyear
dfin$actual.reproductive.rate[dfin$total.cows==0]<-0

# sum up all the crops eaten over the whole time to calculate crop eaten per half hour
crop.eaten<-data.frame(bs.run=rownames(tapply(d$crop.eaten,d$bs.run,sum)),
	total.crop.eaten=as.numeric(tapply(d$crop.eaten,d$bs.run,sum)))
dfin<-merge(dfin,crop.eaten,by.x="bs.run",by.y="bs.run")
dfin$crop.eaten.per.half.hour.per.cow<- (dfin$total.crop.eaten * 1000) * (1 / dfin$count.cows.in.crops) * 3 * (1 / 24) * (1 / 2)
dfin$crop.eaten.per.half.hour.per.cow[dfin$count.cows.in.crops==0]<-0

# calculate max and min for woodland
max.woodland<-data.frame(bs.run=names(tapply(d$total.woodland.biomass,d$bs.run,max)),
	max.woodland.biomass=tapply(d$total.woodland.biomass,d$bs.run,max))
dfin<-merge(dfin,max.woodland, by.x="bs.run", by.y="bs.run")

min.woodland<-data.frame(bs.run=names(tapply(d$total.woodland.biomass,d$bs.run,min)),
	min.woodland.biomass=tapply(d$total.woodland.biomass,d$bs.run,min))
dfin<-merge(dfin,min.woodland, by.x="bs.run", by.y="bs.run")

# calculate max and min for livestock
max.cows<-data.frame(bs.run=names(tapply(d$count.cows,d$bs.run,max)),
	max.cows=tapply(d$count.cows,d$bs.run,max))
dfin<-merge(dfin,max.cows, by.x="bs.run", by.y="bs.run")

min.cows<-data.frame(bs.run=names(tapply(d$count.cows,d$bs.run,min)),
	min.cows=tapply(d$count.cows,d$bs.run,min))
dfin<-merge(dfin,min.cows, by.x="bs.run", by.y="bs.run")

# calculate a max and min harvest (for not saving grain)
max.harvest<-data.frame(bs.run=names(tapply(d$current.harvest,d$bs.run,max)),
	max.harvest=tapply(d$current.harvest,d$bs.run,max))
dfin<-merge(dfin,max.harvest, by.x="bs.run", by.y="bs.run")

min.harvest<-data.frame(bs.run=names(tapply(d$current.harvest,d$bs.run,min)),
	min.harvest=tapply(d$current.harvest,d$bs.run,min))
	dfin<-merge(dfin,min.harvest, by.x="bs.run", by.y="bs.run")

# calculate a max percent crop eaten
d$percent.crop.eaten<-100 * d$crop.eaten / ( d$current.harvest + d$crop.eaten ) 
d$percent.crop.eaten[( d$current.harvest + d$crop.eaten ) ==0]<-0
max.percent.crop.eaten<-data.frame(bs.run=names(tapply(d$percent.crop.eaten,d$bs.run,max)),
	max.percent.crop.eaten=tapply(d$percent.crop.eaten,d$bs.run,max))
dfin<-merge(dfin,max.percent.crop.eaten, by.x="bs.run", by.y="bs.run")

# calculate an average cows, harvest, and woodland
mean.harvest<-data.frame(bs.run=names(tapply(d$current.harvest,d$bs.run,mean)),
	mean.harvest=tapply(d$current.harvest,d$bs.run,mean))
dfin<-merge(dfin,mean.harvest, by.x="bs.run", by.y="bs.run")
mean.cows<-data.frame(bs.run=names(tapply(d$count.cows,d$bs.run,mean)),
	mean.cows=tapply(d$count.cows,d$bs.run,mean))
dfin<-merge(dfin,mean.cows, by.x="bs.run", by.y="bs.run")
mean.woodland<-data.frame(bs.run=names(tapply(d$total.woodland.biomass,d$bs.run,mean)),
	mean.woodland.biomass=tapply(d$total.woodland.biomass,d$bs.run,mean))
dfin<-merge(dfin,mean.woodland, by.x="bs.run", by.y="bs.run")


#-------------------------------------------------
#  SET SUSTAINABILITY THRESHOLD AND GENERATE DATA
#-------------------------------------------------

outfiles[[1]][outfiles[[1]]$X.run.number.==2,] #that's just one run from one file

[outfiles[[1]]$total.woodland.biomass<muonde.thresh["woodlands"],]


muonde.thresh<-c(50,280,48)
names(muonde.thresh)<-c("cows","woodlands","crops")
#note that's in metric tons for woodlands and crops
other.thresh<-c(1,5.6,0.96)
names(other.thresh)<-c("cows","woodlands","crops")

d.split<-split(d,d$bs.run)
d.split[[2]]$current.harvest<other.thresh[["crops"]]
d.split[[2]]$current.harvest<muonde.thresh[["crops"]]
d.split[[2]]$total.woodland.biomass<other.thresh[["woodlands"]]
d.split[[2]]$total.woodland.biomass<muonde.thresh[["woodlands"]]
d.split[[2]]$count.cows<other.thresh[["cows"]]
d.split[[2]]$count.cows<muonde.thresh[["cows"]]

#this gets the first year that the threshold isn't met
sort(d.split[[2]]$calendar.year[d.split[[2]]$total.woodland.biomass<muonde.thresh[["woodlands"]] ])[1]



#take the original data files and apply a threshold to them
applyThresholds<-function(dat,thresh,save.harvest){

	#apply threshold
	didnt.meet.woodl<-dat$total.woodland.biomass<thresh[["woodlands"]]
	didnt.meet.cows<-dat$count.cows<thresh[["cows"]]
	if(save.harvest){
	didnt.meet.harvest<-dat$mean.previous.harvests.list<thresh[["crops"]]
	} else {
		didnt.meet.harvest<-dat$current.harvest<thresh[["crops"]]
	}
	print(didnt.meet.woodl)
	print(didnt.meet.harvest)
	print(didnt.meet.cows)
}

applyThresholds(d.split[[2]],muonde.thresh,FALSE)

#in the end you want this to be a lapply, you can test it with d.split[2]

#how do you want to combine the three thresholds?  do you want to record the earliest year 
#each of them failed, and whether each of them failed?
#should you output both the save crops true result and the save crop false result?
# maybe have two columns returned for that?  later make it into another data.frame?
#is it even okay to include those results, are they independent enough from the one without
#the saving grain?  maybe those should have stayed independent, twice the runs...
#i'm thinking this shoul dmaybe operate on a dataframe row
#



#add all the summarizing columns to them





#-------------------------
# CHECK VAR DISTRIBUTIONS
#-------------------------


#-------------------------
#  LOGIC CHECKS
#-------------------------


#-------------------------
#   CALIBRATION CHECK
#-------------------------

#-------------------------
# CHECK MUONDE THRESHOLDS
#-------------------------

# and sensitivity of other thresholds

#-------------------------
#   SENSITIVITY CHECK
#-------------------------


