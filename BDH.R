setwd("E:/courses/CS8803/project")

library(caret)
library(reshape)
set.seed(12345)

##################################################################################
## load data files
##################################################################################
data = read.csv('events3.csv', sep=';')
aed = read.csv('aed_list.txt', header=FALSE)

##################################################################################
## data preparation
##################################################################################
event_id = colsplit(data$event_id, split='_', names=c('event','id'))
data1 = cbind(data,event_id)
data1$event_time = as.Date(data1$event_time)
data1$id = as.character(data1$id)

## get patients with AED
aed_id = merge(data1, aed, by.x='id', by.y='V1')
ids = sort(unique(aed_id$patient_id))
events =data1[data1$patient_id %in% ids,]
encounter = events[! events$event %in% c('YOB','GENDER'), ]
  
event1 = events[events$event %in% c('YOB','GENDER'), c('patient_id', 'event_time', 
                                                       'event_value', 'event', 'id')]
event2 = aed_id[,c('patient_id', 'event_time', 'event_value', 'event', 'id')]
event3 = aggregate(encounter$event_time, by=list(encounter$patient_id), FUN=min)
  
combine = rbind(event1, event2)
raw_feature = combine[order(combine$patient_id, combine$event_time,combine$id,combine$event),]


##################################################################################
## feature extraction
##################################################################################
features = data.frame(matrix(ncol=18, nrow=length(ids)))
names(features)= c('patient_id','age','gender','sick_age','delay_time','aed1_dose','aed1_time'
                   ,'aed2_dose','aed2_time','aed3_dose','aed3_time','aed4_dose','aed4_time'
                   ,'aed5_dose','aed_num','total_dose','total_time','DRE')
count=0
mdate = as.Date('2113-12-14')

for (i in ids) {
  count = count+1
  pid = raw_feature[raw_feature$patient_id == i,]
  ftime = event3[event3$Group.1 == i, 2]
  features[count,8:14]  = 0
  features[count,1]= i
  features[count,2]=  mdate-pid$event_time[2]
  features[count,3]=  pid$event_value[1]
  features[count,4]=  pid$event_time[3] - pid$event_time[2]
  features[count,5]=  pid$event_time[3] - ftime
  
  len = dim(pid)[1]
  aed_cnt =0
  cnt = 0
  total_dose =0
  drug_list = list()
  for (j in seq(3, len,3)){
    if(!pid$id[j] %in% drug_list) {
      aed_cnt=aed_cnt+1
      drug_list = c(drug_list,pid$id[j])
    }
    cnt=cnt+1
    total_dose = total_dose + pid$event_value[j+1]
    if (j <= 15) { 
      features[count,cnt*2+4]=pid$event_value[j+1]
      if (j+3 < len) {
          features[count,cnt*2+5]=pid$event_time[j+3] -pid$event_time[j]
        }
      else {features[count,cnt*2+5] = pid$event_value[j+2]}
    }
  }
  features[count,15]= aed_cnt
  features[count,16]= total_dose
  features[count,17]= features[count,7]+features[count,9]+features[count,11]+features[count,13]
  if (aed_cnt >=4){features[count,18]= 1}
  else {features[count,18]= 0}
}
## remove 10 abnormal records
features3 = features[features$sick_age !=0,]
write.csv(features3, file='features.csv', row.names=FALSE)

##--------------------------------------
## feature plot: Figure 2
##--------------------------------------
feature = read.csv('features.csv', sep=',')
featurePlot(x=feature[,2:17],y=jitter(feature[,18]), plot='scatter',span=.5, layout=c(4,4))

##resampling to balance the two classes
f0=feature[feature$DRE==0,]
f1=feature[feature$DRE==1,]
rf2=f1[sample(seq(283), 5493, replace=TRUE),]
rfeature = rbind(f0,rf2)

## center and scale
fs = preProcess(rfeature[,c(2:14,16,17)], method=c('center','scale'))
csfeature = predict(fs, rfeature[,c(2:14,16,17)])
tag = as.factor(rfeature[,18])

##################################################################################
## feature selection
##################################################################################
##correlation
cm = cor(csfeature)
highcor = findCorrelation(cm, cutoff=0.75)

## Figure 3
control <- rfeControl(functions=rfFuncs, method="cv", number=5)
results <- rfe(csfeature, tag, size=c(1:15),rfeControl=control)
print(results)
predictors(results)
plot(results, type=c("g", "o"), xlab='Features')

##pca total, Figure 4
pca = prcomp(csfeature, center = TRUE, scale =TRUE)
library(ggbiplot)
rm(g)
g <- ggbiplot(pca, choices=c(1,3), obs.scale = 1, var.scale = 1, groups = tag, 
              ellipse = TRUE, circle = TRUE)
g <- g + theme(legend.direction = 'horizontal', legend.position = 'top')
g <- g + xlim(-10, 5) + ylim(-5,5)
print(g)


##pca DRE, Figure not shown
pca = prcomp(f1, center = TRUE, scale =TRUE)
rm(g)
g <- ggbiplot(pca, choices=c(1,2), obs.scale = 1, var.scale = 1, groups = as.factor(f1[,18]))
g <- g + theme(legend.direction = 'horizontal', legend.position = 'top')
g <- g + xlim(-4, 4) + ylim(-4,4)
print(g)


##################################################################################
## DRE prediction
##################################################################################
library(randomForest)
set.seed(12345)
rfdata = cbind(csfeature[,1:8], tag)
rf=randomForest(tag~.,data=rfdata, mtry=3, importance=TRUE)
# table 1: confusion matrix
print(rf)

# Figure 5. importance plot
barplot(importance(rf)[,4], ylab='MeanDecreasedGini', main='Importance of Features')

##################################################################################
## K-means clustering
##################################################################################
library(kmeans)
ks = preProcess(feature[,c(2:14,16,17)], method=c('center','scale'))
kmfeature = predict(ks, feature[,c(2:14,16,17)])
kmdata = kmfeature[feature$DRE==1, ]

# K=3
fit3 = kmeans(kmdata,3)
kmout3 = data.frame(kmdata, fit3$cluster)
# Figure 6, Top
library(cluster) 
clusplot(kmdata, fit3$cluster, color=TRUE, shade=TRUE, labels=1, lines=0, 
         main='DRE clusters: K=3', xlab='PC1', ylab='PC2' )


## k=2
fit2 = kmeans(kmdata,2)
kmout2 = data.frame(kmdata, fit2$cluster)
# Figure 6, bottom
clusplot(kmdata, fit2$cluster, color=TRUE, shade=TRUE, labels=1, lines=0, 
         main='DRE clusters: K=2', xlab='PC1', ylab='PC2' )


## comparions between Cluster 1 and Cluster 2. Table 2
cluster1 = kmout2[kmout2$fit2.cluster==1,] 
cluster2 = kmout2[kmout2$fit2.cluster==2,] 

t.test(cluster1$age, cluster2$age)
t.test(cluster1$gender, cluster2$gender)
t.test(cluster1$sick_age, cluster2$sick_age)
t.test(cluster1$delay_time, cluster2$delay_time)
t.test(cluster1$aed1_dose, cluster2$aed1_dose)
t.test(cluster1$aed1_time, cluster2$aed1_time)
t.test(cluster1$aed2_dose, cluster2$aed2_dose)
t.test(cluster1$aed2_time, cluster2$aed2_time)
t.test(cluster1$aed3_dose, cluster2$aed3_dose)
t.test(cluster1$aed3_time, cluster2$aed3_time)
t.test(cluster1$aed4_dose, cluster2$aed4_dose)
t.test(cluster1$aed4_time, cluster2$aed4_time)
t.test(cluster1$aed5_dose, cluster2$aed5_dose)
t.test(cluster1$total_time, cluster2$total_time)
t.test(cluster1$total_dose, cluster2$total_dose)
