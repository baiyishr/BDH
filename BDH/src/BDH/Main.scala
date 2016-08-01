package BDH

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._


import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import java.io._       
import sys.process._

object Main {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sf = new SparkConf().setAppName("BDH").setMaster("local[6]")
    val sc = new SparkContext(sf)
    val sqc = new SQLContext(sc)
    import sqc.createSchemaRDD
    
    //----------------------------------------------------
    //load data
    //----------------------------------------------------
    val events = sc.textFile("./event3.csv").map(x=>eventparse(x))
    val aed =  sc.textFile("./aed_list.csv").collect
                    
    val aed_patient = events.filter(x => aed.contains(x._3)).map(_._1).distinct

     val aed_events = events.map(x => (x._1, x)).join(aed_patient.map(x=>(x,x)))
                       .map(x=>x._2._1)
                       
     val raw_feature =  aed_events.filter(x=> (x._2 == "YOB") || (x._2 == "GENDER") || (x._2 == "MOLECULE") 
                                   || (x._2 == "QUANTITY") || (x._2 == "SUPPLYDAYS"))
                       .sortBy(x=>(x._1,x._4, x._3, x._2))
     val min_date =  aed_events.filter(x => ! (x._2 == "YOB" || x._2 == "GENDER"))
                       .groupBy(_._1).map(x=> x._2.minBy(_._4.asInstanceOf[Date]))
                       .map(x=>(x._1,x._4))
     
     //----------------------------------------------------                  
     // feature extraction
     //----------------------------------------------------
     val features = raw_feature.groupBy(_._1).join(min_date).map(x=> (x._1,featureExtraction(x._2._1.toList, x._2._2)))
                       .map(x=>(x._1, x._2._1,  x._2._2, x._2._3, x._2._4, x._2._5, x._2._6, x._2._7, x._2._8, x._2._9,
                            x._2._10, x._2._11, x._2._12, x._2._13, x._2._14, x._2._15, x._2._16, x._2._17))

     //----------------------------------------------------
     // randomforest
     //----------------------------------------------------
     val df = features.map(x => LabeledPoint(x._18, Vectors.dense(x._2, x._3, x._4, x._5, x._6, x._7,x._7,x._9)))
       
       
       val splits = df.randomSplit(Array(0.7,0.3),seed=11111)
       val (train, test) = (splits(0), splits(1))
       train.cache()
       test.cache()

         val numClasses = 2
         val categoricalFeaturesInfo = Map[Int,Int]()
         val numTrees = 500
         val featureSubsetStrategy = "auto"
         val impurity = "entropy"
         val maxDepth = 12
         val maxBins = 32
       
         val model = RandomForest.trainClassifier(train, numClasses, categoricalFeaturesInfo, numTrees, 
                                                 featureSubsetStrategy,impurity,maxDepth,maxBins, seed=12345)             
         
       // testing model
       val trainErr = train.map{p => 
               val prediction = model.predict(p.features)
               if (p.label == prediction) 1.0 else 0.0    
       }.mean()
       
       val testErr = test.map{p => 
               val prediction = model.predict(p.features)
               if (p.label == prediction) 1.0 else 0.0    
       }.mean()

  }
   

      
  def eventparse(line:String):(String, String, String, Date, Float)={
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val fields = line.split(";")
    val id = fields(0).trim
    val event = fields(1).trim.toUpperCase
    val event_ = event.split("_",2)
    val code = if (event_.size > 1) event_(1).trim.toUpperCase else null
    val date = dateFormat.parse(fields(2))
    val value = fields(3).toFloat
    (id, event, code, date, value)
  }
    
  def featureExtraction(list:List[(String, String, String, Date, Float)], mindate:Date)={
    val len = list.size
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val mdate = dateFormat.parse("2113-12-14")
    val gender = list(0)._5
    val age = (mdate.getTime() - list(1)._4.getTime())/(60*60*24*1000.00)
    val sick_age = (list(2)._4.getTime() - list(1)._4.getTime())/(60*60*24*1000.00)
    val delay_time = (list(2)._4.getTime() - mindate.getTime())/(60*60*24*1000.00)
    val aed1_dose = list(4)._5
    val aed1_time = if (len >6) (list(6)._4.getTime() - list(3)._4.getTime())/(60*60*24*1000.00) else list(5)._5
    val aed2_dose = if (len >6) list(4)._5 else 0
    val aed2_time = if (len >9) (list(9)._4.getTime() - list(6)._4.getTime())/(60*60*24*1000.00) else list(8)._5
    val aed3_dose = if (len >9) list(7)._5 else 0
    val aed3_time = if (len >12) (list(12)._4.getTime() - list(9)._4.getTime())/(60*60*24*1000.00) else list(11)._5
    val aed4_dose = if (len >12) list(10)._5 else 0
    val aed4_time = if (len >15) (list(15)._4.getTime() - list(12)._4.getTime())/(60*60*24*1000.00) else list(14)._5
    val aed5_dose = if (len >15) list(13)._5 else 0
    var aed_cnt = Set("")
    for (i <- 3 to len){
      aed_cnt += list(i)._3
    }
      
    val aed_num = aed_cnt.size-1
    val total_dose = aed1_dose+aed2_dose+aed3_dose+aed4_dose+aed5_dose
    val total_time = aed1_time+aed2_time+aed3_time+aed4_time
    val DRE = if (aed_num>=4) 1 else 0
    (age, gender, sick_age, delay_time, aed1_dose, aed1_time,aed2_dose, aed2_time,aed3_dose, aed3_time,aed4_dose, aed4_time,
        aed5_dose, aed_num, total_dose, total_time, DRE)
  }
      
 }