// object Function{

import com.mongodb.spark._
import com.mongodb.spark.config._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import com.mongodb.spark.rdd.MongoRDD
import com.mongodb.spark.sql._
import org.bson.Document
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import java.sql.Timestamp
import java.time.Duration
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Column
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.mutable
import org.apache.spark.sql.types.{StructType,StructField,StringType}

val base ="mongodb://127.0.0.1/cpmongo."
val output_base = "mongodb://127.0.0.1/cpmongo.USER_SIMILARITY"
val USER_LIST_FOR_SIM_output_base = "mongodb://127.0.0.1/cpmongo.USER_LIST_FOR_SIMILARITY"
val USER_SIM_output_base = "mongodb://127.0.0.1/cpmongo.USER_SIMILARITY"
val SREG_output_base = "mongodb://127.0.0.1/cpmongo.SREG_SIM"
val NCR_output_base = "mongodb://127.0.0.1/cpmongo.NCR_SIM"
val ACT_output_base = "mongodb://127.0.0.1/cpmongo.ACTIVITY_SIM"
//교과: SREG_SIM, 비교과: NCR_SIM, 자율활동: ACTIVITY_SIM
val Result_output_base = "mongodb://127.0.0.1/cpmongo.Recommend_Result"

// val sparkConf = new SparkConf().setAppName("Empty-DataFrame").setMaster("local")
// val sc = new SparkContext(sparkConf) //쉘 외 환경에서 실행할 경우 주석 제거 필요

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

val comRatingUri = "CPS_RATING"
val sjobSrhUri = "CPS_EMPLOY_SEARCH_HIS"
val replyUri = "CPS_BOARD_REPLY"  //댓글
val codeUri = "CPS_CODE_MNG"  //통합 코드관리 테이블
val gradCorpUri = "CPS_GRADUATE_CORP_INFO"  //졸업 기업 // <<관심기업팀>>
val ncrInfoUri = "CPS_NCR_PROGRAM_INFO"  //비교과 정보
val ncrStdInfoUri = "CPS_NCR_PROGRAM_STD"  //비교과 신청학생
val outActUri = "CPS_OUT_ACTIVITY_MNG"  //교외활동
val jobInfoUri = "CPS_SCHOOL_EMPLOY_INFO"  //채용정보-관리자 등록 // <<관심기업팀>>
val sjobInfoUri = "CPS_SCHOOL_EMPLOY_STD_INFO"  //채용정보 신청 학생 정보(student job info) // <<관심기업팀>>
val deptInfoUri = "V_STD_CDP_DEPT"  //학과 정보 (department info)
val clPassUri = "V_STD_CDP_PASSCURI" //교과목 수료(class pass)
val stInfoUri = "V_STD_CDP_SREG"  //학생 정보 (student info) // <<관심기업팀>>
val pfInfoUri = "V_STD_CDP_STAF"  //교수 정보 (professor info)
val clInfoUri = "V_STD_CDP_SUBJECT"  //교과 정보 (class info)
val cpsStarUri = "CPS_STAR_POINT"  //교과/비교과용 별점 테이블
// val userforSimilarityUri = "USER_LIST_FOR_SIMILARITY" //유사도 분석 팀이 생성한 테이블

Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)
Logger.getLogger("MongoRelation").setLevel(Level.OFF)
Logger.getLogger("MongoClientCache").setLevel(Level.OFF)
Logger.getRootLogger().setLevel(Level.ERROR)

//함수 정의
def getMongoDF(
                spark : SparkSession,
                coll : String ) : DataFrame = {
  spark.read.mongo(ReadConfig(Map("uri"->(base+coll))))
}

//저장하기 setMongo(spark, Uri, dataframe)으로 사용
def setMongoDF(
                spark : SparkSession,
                coll: String,
                df : DataFrame ) = {
  df.saveToMongoDB(WriteConfig(Map("uri"->(base+coll))))
}



//추천결과팀 데이터 저장
def setMongoDF_result(
                       spark : SparkSession,
                       df : DataFrame ) = {
  df.saveToMongoDB(WriteConfig(Map("uri"->(Result_output_base))))
}

def setMongoDF_USER_LIST(
spark : SparkSession,
df : DataFrame ) = {
df.saveToMongoDB(WriteConfig(Map("uri"->(USER_LIST_FOR_SIM_output_base))))
}

def setMongoDF_USER_SIM(
spark : SparkSession,
df : DataFrame ) = {
df.saveToMongoDB(WriteConfig(Map("uri"->(USER_SIM_output_base))))
}

//몽고DB에서 컬렉션 읽어오기
val replyUri_table = getMongoDF(spark, replyUri)  //댓글
val codeUri_table =  getMongoDF(spark, codeUri)  //통합 코드관리 테이블
val gradCorpUri_table =  getMongoDF(spark, gradCorpUri)  //졸업 기업
val ncrInfoUri_table =  getMongoDF(spark, ncrInfoUri)  //비교과 정보
val ncrStdInfoUri_table =  getMongoDF(spark, ncrStdInfoUri)  //비교과 신청학생
val outActUri_table =  getMongoDF(spark, outActUri)  //교외활동
val jobInfoUri_table =  getMongoDF(spark, jobInfoUri)  //채용정보-관리자 등록
val sjobInfoUri_table =  getMongoDF(spark, sjobInfoUri)  //채용정보 신청 학생 정보(student job info)
val deptInfoUri_table =  getMongoDF(spark, deptInfoUri)  //학과 정보 (department info)
val clPassUri_table =  getMongoDF(spark, clPassUri) //교과목 수료(class pass)
val stInfoUri_table =  getMongoDF(spark, stInfoUri)  //학생 정보 (student info)
val pfInfoUri_table =  getMongoDF(spark, pfInfoUri)  //교수 정보 (professor info)
val clInfoUri_table =  getMongoDF(spark, clInfoUri)  //교과 정보 (class info)

val cpsStarUri_table = getMongoDF(spark, cpsStarUri)  //교과/비교과용 별점 테이블

//object Function{ 기존 옵젝 선언 위치
  //관심기업 함수
  def INTEREST(std_no:Int):(DataFrame, DataFrame, DataFrame)={
    def mkjobComList (spark:SparkSession, stdNo: Int) : DataFrame = {
    	//학생-채용 신청 정보 테이블
    	val getsjobInfo = getMongoDF(spark, sjobInfoUri)
    	val sjob = getsjobInfo.select('SCE_INFO_KEY_ID, 'REG_DATE, 'SCE_STD_NO, 'SCE_STD_APPLY_YN).distinct()
    	val sjobFilter = sjob.filter(sjob("SCE_STD_APPLY_YN") === "Y")
    	val std_sjobList= sjobFilter.filter(sjobFilter("SCE_STD_NO") === stdNo)
    	//채용정보 테이블
    	val getjobInfo = getMongoDF(spark, jobInfoUri)
    	val jobInfo = getjobInfo.select('SCE_KEY_ID.as("SCE_INFO_KEY_ID"), 'SCE_COMP_TITLE, 'SCE_SECTORS, 'SCE_RELATED_DEPTS).distinct().orderBy("SCE_KEY_ID")
    	//학생-채용신청-회사 정보 리스트 생성	//REG_DATE기준으로 최신순으로 정렬
    	val jobComList = std_sjobList.join(jobInfo, Seq("SCE_INFO_KEY_ID"), "inner").orderBy("REG_DATE")
    	val comList = jobComList.select('SCE_COMP_TITLE).withColumnRenamed("SCE_COMP_TITLE", "ComList")
    	comList
    }

    def mkjobSearch (spark:SparkSession, stdNo: Int) : DataFrame = {
    	//채용정보 조회 이력 테이블
      	val getsjobSrh = getMongoDF(spark, sjobSrhUri)
      	val std_srhList = getsjobSrh.select('ESH_KEY_ID, 'SEARCH_PARAM, 'REG_ID, 'REG_DATE, 'CORP_NM).distinct().orderBy(desc("REG_DATE")).filter('REG_ID === stdNo)
    	val srhList = std_srhList.select('CORP_NM).withColumnRenamed("CORP_NM", "ComList")
    	srhList
    }

    def mkDepGenComList (spark:SparkSession, stdNo : Int) : DataFrame = {
    	val  schema_string = "ComList"
    	val schema_rdd = StructType(schema_string.split(",").map(fieldName => StructField(fieldName, StringType, true)) )
    	val getGstdInfo = getMongoDF(spark, gradCorpUri)
    	val getStdInfo = getMongoDF(spark, stInfoUri)
    	//std_no를 이용하여 getStdInfo 에서 일치하는 학생 정보(학번, 이름, 학과명, 성별, 졸업여부)만 추출
        val stdInfo = getStdInfo.select('STD_NO, 'KOR_NM, 'SUST_CD_NM, 'GEN_FG_NM, 'GRADT_YN).filter('STD_NO === stdNo)
    	//질의 해당 학생의 성별, 학과
    	try{
    		var stdGen = stdInfo.select('GEN_FG_NM).first.getString(0)
    		if (stdGen == "남성")
    			stdGen = "남"
    		else
    			stdGen = "여"
    		val stdDep = stdInfo.select('SUST_CD_NM).first.getString(0)
    		//select를 통해 졸업자 취업리스트(학번, 학과, 성별, 기업명 리스트) 생성
    		val gStdInfo = getGstdInfo.select('GCI_STD_NO, 'GCI_SUST_CD_NM, 'GCI_GEN_FG_NM, 'GCI_CORP_KEY_ID, 'GCI_CORP_NM).distinct()
    		//성별, 학과 모두 일치하는 기업리스트 생성
    		val gStdInfoList = gStdInfo.filter('GCI_GEN_FG_NM === stdGen).filter('GCI_SUST_CD_NM === stdDep)
    		val gStdComList = gStdInfoList.select('GCI_CORP_NM).withColumnRenamed("GCI_CORP_NM", "ComList")
    		gStdComList
    	}catch{
    		case e: NoSuchElementException => println("ERROR!")
    		val empty_df = sqlContext.createDataFrame(sc.emptyRDD[Row], schema_rdd)
    		empty_df
    	}
    }

    val applyComlist = mkjobComList(spark, std_no)
    val searchComlist = mkjobSearch(spark, std_no)
    val gStdComList = mkDepGenComList(spark, std_no)

    (applyComlist, searchComlist, gStdComList)
  }



  //신뢰도 함수
  def TRUST(applyComlist:DataFrame, searchComlist:DataFrame, gStdComList:DataFrame):(DataFrame, DataFrame)={
    val corps_applyComlist = applyComlist.collect.map(_.toSeq).flatten
    val corps_searchComlist = searchComlist.collect.map(_.toSeq).flatten
    val corps_gStdComList = gStdComList.collect.map(_.toSeq).flatten

    val getGstdInfo = getMongoDF(spark, gradCorpUri) //test
    val getGstdInfo_ = getGstdInfo.select("*") //rdd2

    //categ# 함수는 기업을 카테고리 별로 분류하기 위한 함수
    def categ1(corps_applyComlist: Array[Any]):scala.collection.mutable.Map[Any, Array[org.apache.spark.sql.Row]]={
         var a = scala.collection.mutable.Map[Any, Array[org.apache.spark.sql.Row]]()
         for(i<-0 until corps_applyComlist.size){
         a = a+(corps_applyComlist(i)->getGstdInfo_.filter(getGstdInfo_("GCI_CORP_NM")===corps_applyComlist(i)).collect)
         }
         a}

    def categ2(corps_searchComlist: Array[Any]):scala.collection.mutable.Map[Any, Array[org.apache.spark.sql.Row]]={
         var a = scala.collection.mutable.Map[Any, Array[org.apache.spark.sql.Row]]()
         for(i<-0 until corps_searchComlist.size){
         a = a+(corps_searchComlist(i)->getGstdInfo_.filter(getGstdInfo_("GCI_CORP_NM")===corps_searchComlist(i)).collect)
         }
         a}

    def categ3(corps_gStdComList: Array[Any]):scala.collection.mutable.Map[Any, Array[org.apache.spark.sql.Row]]={
         var a = scala.collection.mutable.Map[Any, Array[org.apache.spark.sql.Row]]()
         for(i<-0 until corps_gStdComList.size){
         a = a+(corps_gStdComList(i)->getGstdInfo_.filter(getGstdInfo_("GCI_CORP_NM")===corps_gStdComList(i)).collect)
         }
         a}

    val corps1 = categ1(corps_applyComlist)
    val corps2 = categ2(corps_searchComlist)
    val corps3 = categ3(corps_gStdComList)

    var final_corps = scala.collection.mutable.Map[Any, Array[org.apache.spark.sql.Row]]() //최종 기업 목록

    if(corps1.size >= 10){
    	final_corps = corps1
    } else if(corps1.size + corps2.size >= 10){
    	final_corps = corps1 ++ corps2
    	} else{
    	final_corps = corps1 ++ corps2 ++ corps3
    	}

    val test2 = getMongoDF(spark, "CPS_RATING")
    var tuples = Seq[(Int, Double)]()
    val t5 = getGstdInfo.select(col("GCI_STD_NO"), col("GCI_CORP_NM")).distinct.toDF

    var final_corps_keys = final_corps.keys.toList

    //---------------------사용자 신뢰도(재용)---------------------------


    for(i<-0 until final_corps.size){
      val a = 0.2
      val b = 0.125
      val w1 = 0.5
      val w2 = 0.5
      val columns = Seq("GCI_STD_NO", "RATING")
      var df = test2.filter(test2("기업명")===final_corps_keys(i)).toDF
      var filter = t5.filter(t5("GCI_CORP_NM").equalTo(final_corps_keys(i))).collect
      if(df.collect.size>0){
        var add = df.select("기업가중치").as[String].collect()(0).toInt*w1*a+df.select("직원수 가중치").as[String].collect()(0).toInt*w2*b
        for(j<-0 until filter.size){
          tuples = tuples :+ (filter(j)(0).toString.toInt, add.toString.toDouble)
          }
        }else{
          for(j<-0 until filter.size){
            tuples = tuples :+ (filter(j)(0).toString.toInt, 0.0)
        }
      }
    }

    val df = tuples.toDF("GCI_STD_NO", "TRUST")

    val test0 = getMongoDF(spark, "CPS_STAR_POINT")
    var con1 = test0.select(col("STAR_KEY_ID"), col("STAR_POINT")).groupBy("STAR_KEY_ID").agg(avg("STAR_POINT").alias("STAR_POINT")) //콘텐츠 신뢰도 데이터프레임

    //코드 다 실행하고 결과 출력
    //df.show()
    //con1.show() //중분류에 대한 신뢰도

    //활동 점수 계산
    //교과 + 비교과 + 자율활동

    val stInfoUri_ = getMongoDF(spark, stInfoUri)
    var st_list = stInfoUri_.limit(5).select("STD_NO").rdd.map(r=>r(0)).collect()

    val gradCorpUri_ = getMongoDF(spark, gradCorpUri)
    val clPassUri_ = getMongoDF(spark, clPassUri)
    val ncrStdInfoUri_ = getMongoDF(spark, ncrStdInfoUri)
    val outActUri_ = getMongoDF(spark, outActUri)
    val starpoint_ = getMongoDF(spark, "CPS_STAR_POINT")
    var out_act_score = Seq[(Int, Double)]()

    //아래 for문 시간 좀 걸림.
    for(i<-0 until st_list.size){ //학생들의 활동 점수 계산을 위해 영역 별로 count를 수행하여 DF로 만들 것
      var sbjt = clPassUri_.filter(clPassUri_("STD_NO")===st_list(i)).toDF
      var ncr = ncrStdInfoUri_.filter(ncrStdInfoUri_("NPS_STD_NO")===st_list(i)).toDF
      var out = outActUri_.filter(outActUri_("OAM_STD_NO")===st_list(i)).toDF

      var stars = starpoint_.filter(starpoint_("STD_NO")===st_list(i)).count //st_list(i) 학생이 교과/비교과에 준 총 별점의 개수

      var total = sbjt.count+ncr.count+out.count //학생이 총 수강/활동한 (교과/비교과)/대외활동의 수
      var act_score = 0

      if(total==0){
        act_score = stars.toInt / (total+1).toInt //활동 점수 계산
      }else{
        act_score = stars.toInt / total.toInt //활동 점수 계산
      }

      out_act_score = out_act_score :+ (st_list(i).toString.toInt, act_score.toString.toDouble)

    }

    var out_act_score_df = out_act_score.toDF("STD_NO", "ACT_SCORE")

    //st_list.size
    //st_list(i)

    //기업 점수 df와 활동 점수 df를 내부 조인함
    val df_out_join = df.join(out_act_score_df, df("GCI_STD_NO")===out_act_score_df("STD_NO"), "inner")


    val scores = df_out_join.select(col("STD_NO"), col("TRUST"), col("ACT_SCORE")).collect //GCI_STD_NO를 제외하고 컬럼들 선택

    var user_trust = Seq[(Int, Double)]() //최종 신뢰도가 들어갈 시퀀스

    for(i<-0 until scores.size){
      //scores(i)의 1:학번, 2:기업점수, 3:활동점수가 순서대로 들어감.
      var std_no = scores(i)(0)
      var trust = scores(i)(1)
      var act_score = scores(i)(2)
      val w1 = 0.5
      val w2 = 0.5

      user_trust = user_trust :+ (std_no.asInstanceOf[Int], w1*scores(i)(1).asInstanceOf[Double]+w2*scores(i)(2).asInstanceOf[Double]) //여기서 인자를 추가해서 계산해줘야
    }

    var user_trust_df = user_trust.toDF("STD_NO", "TRUST") //최종 결과를 데이터프레임으로 만듬

    (user_trust_df, con1)
  }



  //============================유사도(연희,소민)start=======================================
  //// 최종 Running Runtime 확인
  import java.util.concurrent.TimeUnit.NANOSECONDS
  def time[T](f: => T): T = {
    val start = System.nanoTime()
    val ret = f
    val end = System.nanoTime() // scalastyle:off println
    println(s"Time taken: ${NANOSECONDS.toMillis(end - start)} ms")
    // scalastyle:on println
    ret
  }

    def calSim (spark:SparkSession, std_NO: Int) : DataFrame = {
      var clPassUri_DF = clPassUri_table.select(col("STD_NO"), col("SUST_CD_NM"), col("SBJT_KOR_NM"), col("SBJT_KEY_CD")).distinct.toDF
      var cpsStarUri_DF = cpsStarUri_table.select(col("STD_NO"), col("STAR_KEY_ID"), col("STAR_POINT"), col("TYPE"))
      var cpsStarUri_DF_ncr = cpsStarUri_table.select(col("STD_NO"), col("STAR_KEY_ID").as("NPI_KEY_ID"), col("STAR_POINT"), col("TYPE"))
      //########################### NPI_KEY_ID를 NPI_KEY_ID_NCR 이름으로 가져옴##############################################
      var ncrInfoUri_DF = ncrInfoUri_table.select(col("NPI_KEY_ID").as("NPI_KEY_ID_NCR"), col("NPI_AREA_SUB_CD"))
      var outActUri_DF = outActUri_table.select(col("OAM_STD_NO"), col("OAM_TYPE_CD"), col("OAM_TITLE"))

      //질의자 학번 들어오면 학과 찾아서 departNM 생성
      val schema_string = "Similarity"
      val schema_rdd = StructType(schema_string.split(",").map(fieldName => StructField(fieldName, StringType, true)) )

      var departNM_by_stdNO = spark.createDataFrame(sc.emptyRDD[Row], schema_rdd)
      try{
        departNM_by_stdNO = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${std_NO}")).select(col("SUST_CD_NM")).distinct
      }
      catch{
        case e: NoSuchElementException => println("ERROR!")
        val empty_df = sqlContext.createDataFrame(sc.emptyRDD[Row], schema_rdd)
        empty_df
      }
      var departNM = departNM_by_stdNO.collect().map(_.getString(0)).mkString("")

      var stdNO_in_departNM_sbjt = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}")).select(col("STD_NO")).distinct
      var stdNO_in_departNM_List = stdNO_in_departNM_sbjt.rdd.map(r=>r(0)).collect.toList.slice(0, 100).map(_.toString)
      // var stdNO_in_departNM_List = stdNO_in_departNM_sbjt.rdd.map(r=>r(0)).collect.toList.map(_.toString)
      var sbjtCD_in_departNM = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}")).select("SBJT_KEY_CD", "STD_NO")
      var sbjtCD_in_departNM_List = sbjtCD_in_departNM.rdd.map(r=>r(0).toString).collect.toList.distinct.sorted
      var sbjtNM_by_stdNO = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${std_NO}")).select(col("SBJT_KEY_CD"))
      var sbjtNM_by_stdNO_List = sbjtNM_by_stdNO.rdd.map(r=>r(0)).collect.toList.distinct
      var cpsStarUri_DF_ncr_typeN = cpsStarUri_DF_ncr.filter(cpsStarUri_DF_ncr("TYPE").equalTo("N"))
      // 비교과 별점 => 별점 테이블에서 "TYPE"이 "N" :: ex) NCR000000000677
      // var cpsStarUri_DF_ncr_typeN = cpsStarUri_DF_ncr.filter(cpsStarUri_DF_ncr("TYPE").equalTo("N"))
      // 교과 별점 => 별점 테이블에서 "TYPE"이 "C" :: ex) BAQ00028






      //-------------------- # # # 교과목 리스트 # # # --------------------------------
      //--------------------from. 교과목수료 테이블 : 학과명, 학번, 학점----------------------

      def sbjtFunc(spark:SparkSession, std_NO: Int) : DataFrame = {

      var cpsStarUri_sbjt_DF = cpsStarUri_DF.filter(cpsStarUri_DF("TYPE").equalTo("C")).drop("TYPE")

      case class starPoint(sbjtCD:String, starpoint:Any)

      var sbjtCD_star_byStd_Map = stdNO_in_departNM_List.flatMap{ stdNO =>
        // 학번 별 학번-교과코드-별점
        val star_temp_bystdNO_DF = cpsStarUri_sbjt_DF.filter(cpsStarUri_sbjt_DF("STD_NO").equalTo(s"${stdNO}"))

        val res =
          star_temp_bystdNO_DF
          .collect // dataframe 를 collect 해서 array[row] 를 받음
          .map(record => (record(0).toString, starPoint(record(1).toString, record(2).toString)))
          // 각 row에 대해서 (학번, starPoint 객체) 로 바꿈
          .groupBy(x => x._1) // 이걸 하면 데이터 타입이 맵(학번, )
          .map( x => (x._1, x._2.map(x => x._2)))
          // 그룹바이를 학번으로 해서 (학번, Array)
        res
      }.toMap

      var sbjt_tuples = Seq[(String, List[Double])]()

      var stdNo_List_byMap_sbjt = sbjtCD_star_byStd_Map.keys.toList

      stdNO_in_departNM_List.foreach{ stdNo =>
        var star_point_List = List[Any]() // 학번당 별점을 저장
        var orderedIdx_byStd = List[Int]() //학번 당 교과 리스트
        var not_orderedIdx_byStd = List[Int]()

        for(i<-0 until sbjtCD_in_departNM_List.size){
            star_point_List = -1.0::star_point_List
        }

        var getStar_by_stdNO = cpsStarUri_sbjt_DF.filter(cpsStarUri_sbjt_DF("STD_NO").equalTo(s"${stdNo}")).select("STAR_KEY_ID").collect.map({row=>
           val str = row.toString
           val size = str.length
           val res = str.substring(1, size-1).split(",")
           val list_ = res(0)
           list_})

        var not_rated = sbjtNM_by_stdNO_List filterNot getStar_by_stdNO.contains
        // println(not_rated)
        // stdNo_List_byMap_sbjt
        if(stdNo_List_byMap_sbjt.contains(s"${stdNo}")){
          var valueBystdNo_from_Map = sbjtCD_star_byStd_Map(s"${stdNo}") //!
          for(i<-0 until valueBystdNo_from_Map.size){
            //학생 한명의 중분류-별점 맵에서 중분류 키에 접근 : valueBystdNo_from_Map(i).subcd)
            //학생 한명이 들은 중분류 리스트를 가져옴
            orderedIdx_byStd = sbjtCD_in_departNM_List.indexOf(valueBystdNo_from_Map(i).sbjtCD)::orderedIdx_byStd
            // println("orderedIdx_byStd ===> " + orderedIdx_byStd)
          }

          for(i<-0 until not_rated.size){
            not_orderedIdx_byStd = sbjtCD_in_departNM_List.indexOf(not_rated(i).toString)::not_orderedIdx_byStd
            // println("not_orderedIdx_byStd ===> " + not_orderedIdx_byStd)
          }

          orderedIdx_byStd = orderedIdx_byStd.sorted
          // println("orderedIdx_byStdsorted ===>" + orderedIdx_byStd)

          not_orderedIdx_byStd = not_orderedIdx_byStd.sorted
          // println("not_orderedIdx_byStd ===>" + not_orderedIdx_byStd)

          for(i<-0 until not_orderedIdx_byStd.size){
            star_point_List = star_point_List.updated(not_orderedIdx_byStd(i), -1)
          }

          for(i<-0 until orderedIdx_byStd.size){ // orderedIdx_byStd 크기 (학번당 들은 중분류를 for문 돌림)
            var k=0;
            //print(k)
            // 학과 전체의 중분류 리스트와 학생의 중분류 리스트의 값이 같을때까지 k를 증가
            while(sbjtCD_in_departNM_List(orderedIdx_byStd(i))!= valueBystdNo_from_Map(k).sbjtCD){
            k+=1;
            }
            // 같은 값이 나오면 0으로 설정돼있던 값을 (그 자리의 값을) 학생의 별점으로 바꿔줌
            star_point_List = star_point_List.updated(orderedIdx_byStd(i), valueBystdNo_from_Map(k).starpoint)
            // println(s"$star_point_List")
          }
        }
        val star_list = star_point_List.map(x => x.toString.toDouble)
        // println(">>"+star_list)
        sbjt_tuples = sbjt_tuples :+ (stdNo.toString, star_list)
      }
      var sbjt_df = sbjt_tuples.toDF("STD_NO", "SUBJECT_STAR")
      println("sbjt complete!!")
      sbjt_df
    }


      //======================================================================================================
      //======================================================================================================


      //-------------------- # # # 비교과 리스트 # # # --------------------------------
      // from. 교과/비교과용 별점 테이블(CPS_STAR_POINT) : 학번(STD_NO), 비교과id(STAR_KEY_ID), 별점(STAR_POINT), 타입(TYPE)
      // from. 비교과 관련 테이블(CPS_NCR_PROGRAM_INFO) : 비교과id(NPI_KEY_ID), 중분류(NPI_AREA_SUB_CD)
      //비교과 아이디(학번, 비교과id 사용 from.교과/비교과별점테이블)로 중분류 가져오기(비교과id, 중분류 from.비교과 관련 테이블)
      //학과 별 학생들이 수강한 비교과의 중분류 list 로 포맷 잡고 : 학과 - 학번 돌면서 list 만들고 , 중분류로 바꿔주기
      //학생 한명이 수강한 비교과 list -> 별점 가져오기(from. 교과/비교과 별점 테이블) -> 중분류 가져오기 -> 중분류 별 별점 avg 계산

      def ncrFunc(spark:SparkSession, std_NO: Int) : DataFrame = {

      val schema1 = StructType(
        StructField("STAR_POINT", StringType, true) ::
          StructField("NPI_KEY_ID", StringType, true) :: Nil)

      val schema2 = StructType(
        StructField("NPI_AREA_SUB_CD", StringType, true) ::
          StructField("NPI_KEY_ID", StringType, true) :: Nil)

      val schema3 = StructType(
          StructField("STAR_POINT", StringType, true) ::
          StructField("NPI_KEY_ID", StringType, true) :: Nil)
      var star_subcd_DF = spark.createDataFrame(sc.emptyRDD[Row], schema3)

      val schema4 = StructType(
          StructField("NPI_AREA_SUB_CD", StringType, true) :: Nil)
      var subcd_byStd_DF = spark.createDataFrame(sc.emptyRDD[Row], schema4)
      //----------------------------------------------------------------------------------------------------------------------------

      //-----------------------------------------------<학과의 비교과중분류 리스트 생성>------------------------------------------------
      //map연산은 dataframe에 쓸 수 없기 때문에 list로 변환해야 하며 dataframe을 list로 변환하려면 df의 값 하나하나에 접근하기 위해 map 연산이 필요함
      //광홍과df(clpass 교과목 수료 테이블에서 학과 별 학번 dataframe을 생성한 뒤 list로 변환)
      // Map 타입의 변수 (string, Array)를 인자로 받음
      // String : 학번, Array : (중분류, 별점)
      case class starPoint2(subcd:String, starpoint2:Any)
      val subcd_star_byDepart_Map = collection.mutable.Map[String, Array[starPoint2]]()
      val subcd_byDepart_Map_temp = collection.mutable.Map[String, Array[String]]()
      var subcd_byDepart_List = List[Any]()

      // 학과별 중분류 중복 제거를 위해 Set으로 데이터타입 선언
      val tmp_set = scala.collection.mutable.Set[String]()
      var myResStr = ""
      import spark.implicits._
      val tmp1 = cpsStarUri_DF_ncr_typeN.select(col("NPI_KEY_ID"))
      //val tmp2 = ncrInfoUri_DF.select(col("NPI_AREA_SUB_CD"),col("NPI_KEY_ID"))
      val tmp2 = ncrInfoUri_DF
      // cpsStarUri_DF_ncr_typeN
      // ncrInfoUri_DF
   // var arr01 = Array(20142820, 20142932, 20152611)
   // cpsStarUri_DF_ncr_typeN 는 밖에 있는 코드임
      val star_subcd_joinDF = cpsStarUri_DF_ncr_typeN.join(ncrInfoUri_DF, cpsStarUri_DF_ncr_typeN("NPI_KEY_ID") === ncrInfoUri_DF("NPI_KEY_ID_NCR"), "left_outer").drop("NPI_KEY_ID_NCR")
   // val star_subcd_avg_DF = star_subcd_joinDF.groupBy("NPI_AREA_SUB_CD").agg(avg("STAR_POINT"))

      stdNO_in_departNM_List.foreach { stdNO =>
        // println(stdNO)
        // stdNO = 20152611
        var star_subcd_DF = star_subcd_joinDF.filter(star_subcd_joinDF("STD_NO").equalTo(s"${stdNO}"))
        // star_subcd_DF.show
        var star_subcd_avg_DF = star_subcd_DF.groupBy("NPI_AREA_SUB_CD").agg(avg("STAR_POINT"))
        // star_subcd_avg_DF.show

        val subcd_star_temp = star_subcd_avg_DF.collect.map{ row =>
          val str = row.toString
          val size = str.length
          val res = str.substring(1, size-1).split(",")
          val starP = starPoint2(res(0), res(1))
          starP
        }

          val subcd_star_record = (stdNO.toString, subcd_star_temp)
          // println(subcd_star_record)
          // println(s"star 1 ==== ${subcd_star_temp.mkString(",")} \n ====")
          subcd_star_byDepart_Map+=(subcd_star_record)

          subcd_byStd_DF = star_subcd_DF.select(col("NPI_AREA_SUB_CD"))
          val subcd_byDepart_temp = subcd_byStd_DF.map{ row =>
            // 별점만 가져온거
            // println(row)
            val str = row.toString
            val size = str.length
            val res = str.substring(1, size-1).split(",")(0)
            if(tmp_set.add(res)) {
              // println(s"insert new value : ${res}")
              tmp_set.+(res)
            }
            res
          }.collect().sortBy(x => x)

          val subcd_record_byDepart = (s"$stdNO", subcd_byDepart_temp)
          subcd_byDepart_Map_temp += subcd_record_byDepart

          val t1 = subcd_byDepart_Map_temp.map(x => x._2).flatMap(x => x).toList.distinct
          // val t1 = subcd_byDepart_Map_temp.map(x => x._2).flatMap(x => x).toList.distinct
          // val xres = s"result stdno : ${stdNO} size : ${t1.length} ==--------------------> ${t1}"
          // println(xres)
          // myResStr = myResStr.concat("\n"+xres)

         subcd_byDepart_List = t1

      } //학번 루프 끝

      //----------------------------------------------------------------------------------------------------------------------------

      //최종적인 학번 별 별점 리스트 값이 들어있는 시퀀스
      var ncr_tuples = Seq[(String, List[Double])]()

      var stdNo_List_byMap_ncr = subcd_star_byDepart_Map.keys.toList

      stdNO_in_departNM_List.foreach{ stdNo =>
        var star_point_List = List[Any]() // 학번당 별점을 저장
        var orderedIdx_byStd = List[Int]() //학번 당 중분류 리스트
        //학과 전체 중분류 코드 List => 학번당 별점을 중분류 갯수만금 0.0으로 초기화
        for(i<-0 until subcd_byDepart_List.size){
          star_point_List = -1.0::star_point_List
        }
        //학과 모든 학생의 중분류-별점 Map 에서 학번 하나의 값(중분류-별점)을 가져옴(Map연산을 위해 toString으로 변환)
          if(stdNo_List_byMap_ncr.contains(s"${stdNo}")){
        var valueBystdNo_from_Map = subcd_star_byDepart_Map(s"${stdNo}")
        // println(stdNo)
        // println(valueBystdNo_from_Map)
        //
        for(i<-0 until valueBystdNo_from_Map.size){
          //학생 한명의 중분류-별점 맵에서 중분류 키에 접근 : valueBystdNo_from_Map(i).subcd)
          //학생 한명이 들은 중분류 리스트를 가져옴
          orderedIdx_byStd = subcd_byDepart_List.indexOf(valueBystdNo_from_Map(i).subcd)::orderedIdx_byStd
          // println("orderedIdx_byStd ===> " + orderedIdx_byStd)
        }
        // orderedIdx_byStd를 정렬(중분류 코드 정렬)
        orderedIdx_byStd = orderedIdx_byStd.sorted
        // println("orderedIdx_byStdsorted ===>" + orderedIdx_byStd)

        for(i<-0 until orderedIdx_byStd.size){ // orderedIdx_byStd 크기 (학번당 들은 중분류를 for문 돌림)
          var k=0;
          // print(k)
          // 학과 전체의 중분류 리스트와 학생의 중분류 리스트의 값이 같을때까지 k를 증가
          while(subcd_byDepart_List(orderedIdx_byStd(i))!= valueBystdNo_from_Map(k).subcd){
          k+=1;
          }
          // 같은 값이 나오면 0으로 설정돼있던 값을 (그 자리의 값을) 학생의 별점으로 바꿔줌
          star_point_List = star_point_List.updated(orderedIdx_byStd(i), valueBystdNo_from_Map(k).starpoint2)
          // println(s"$star_point_List")
        }
      }
        val star_list = star_point_List.map(x => x.toString.toDouble)
        // println(">>"+star_list)
        ncr_tuples = ncr_tuples :+ (stdNo, star_list)
    }
      var ncr_df = ncr_tuples.toDF("STD_NO", "NCR_STAR")
      println("ncr complete!!")
      ncr_df
    }
      //===========================================================================================================
      //===========================================================================================================


      def actFunc(spark:SparkSession, std_NO: Int) : DataFrame = {
  //-------------------- # # # 자율활동 리스트 # # # ------------------------------
  //from.교외활동 CPS_OUT_ACTIVITY_MNG : 학번(OAM_STD_NO), 활동구분코드(OAM_TYPE_CD), 활동명(OAM_TITLE)
  var depart_activity_temp = List[Any]()
  var depart_activity_List = List[Any]()
  var activity_List_byStd = List[Any]()
  var act_tuples = Seq[(String, List[Int])]()
  // 자율활동 dataframe 전처리
  var outAct_DF = outActUri_DF.select(col("OAM_STD_NO"), col("OAM_TITLE"), col("OAM_TYPE_CD")).distinct
  //  name을 쓰는 DF, code를 쓰는 DF 분류
  var outAct_name_DF = outAct_DF.filter($"OAM_TYPE_CD" === "OAMTYPCD01" || $"OAM_TYPE_CD" ==="OAMTYPCD02")
  var outAct_code_DF = outAct_DF.filter($"OAM_TYPE_CD" === "OAMTYPCD03" || $"OAM_TYPE_CD" ==="OAMTYPCD04" || $"OAM_TYPE_CD" ==="OAMTYPCD05")

  // 학과 전체학생들의 자율활동 name을 모은 depart_activity_List 를 생성 (학과 학생들이 활동한 자율활동 이름(자격증, 어학)들이 저장됨)
  stdNO_in_departNM_List.foreach{ stdNO =>

    var outAct_name = outAct_name_DF.filter(outAct_name_DF("OAM_STD_NO").equalTo(s"${stdNO}")).select(col("OAM_TITLE"))
    var outAct_name_List = outAct_name.rdd.map(r=>r(0)).collect.toList
    depart_activity_temp = depart_activity_temp ++ outAct_name_List
    //학과 전체 name 리스트
    depart_activity_List = depart_activity_temp.distinct
  }

  // 학과 전체 학생들의 자율활동 name, code에 대한 전체 for문
  stdNO_in_departNM_List.foreach{ stdNO =>
    // 학생들이 활동한 자율활동 code별로 횟수(count)를 위한 작업 (봉사, 대외활동, 기관활동)
    var depart_code_list = List[Any]("OAMTYPCD03", "OAMTYPCD04", "OAMTYPCD05")
    var outAct_code_temp = outAct_code_DF.filter(outAct_code_DF("OAM_STD_NO").equalTo(s"${stdNO}")).select(col("OAM_TYPE_CD"))
    var outAct_code_temp2 = outAct_code_temp.groupBy("OAM_TYPE_CD").count()

    val maps = scala.collection.mutable.Map[String, Int]()

    // map 초기화, 각각의 코드(03, 04, 05)에 대한 Map 값을 0으로 초기화 (길이를 맞추기 위해 (Map(03 -> 0, 04 -> 0, 05 -> 0))
    for(i<-0 until depart_code_list.size){
      maps(depart_code_list(i).toString) = 0
    }
    // 활동한 자율활동에 대해 count 값을 맵핑해줌 ex Array([03,1], [04,1])
    val outAct_code = outAct_code_temp2.collect

    // 활동한 자율활동에 대해 0으로 초기화된 map 에서 해당하는 code 자리에 1, 2 등 위에서 맵핑된 count 값을 넣어줌
    // (Map(03 -> 1, 04 -> 1, 05 -> 0))
    for(i<-0 until outAct_code.size){
      maps(outAct_code(i)(0).toString) = outAct_code(i)(1).toString.toInt
    }
    // 활동 code에 대해 순서를 정렬해줌 (03, 04, 05) 크기 순으로
    val maps_ = maps.toSeq.sortBy(_._1).toMap
    // 각 활동 code에 대해 count Value 만을 뽑아서 List로 반환
    // List(1, 1, 0)
    val code_count_List = maps_.values.toList
    // print("head.size: " + head.size)

    //------------------------------------------------------------------------------

    //---------------------자율활동 name list(자격증01, 어학02)----------------------
    // 학생들이 활동한 자율활동에 name의 목록을 가져오기 위한 작업 (자격증, 어학)
    var outAct_name = outAct_name_DF.filter(outAct_name_DF("OAM_STD_NO").equalTo(s"${stdNO}")).select(col("OAM_TITLE"))
    var outAct_name_List = outAct_name.rdd.map(r=>r(0)).collect.toList

    // 학과 전체 학생들이 활동한 전체 List에 대해서 개개인이 들은 활동 1 또는 0으로 매핑
    // 자신이 활동한 내역이라면 1, 활동하지 않은 내역이라면 0
    var activity_List_byStd_temp1 = depart_activity_List.map(x => (x, 0)).map{ activity =>
      //x : record_1
      //0 : record_2
      //isListend면 1로 바뀜
      val actName = activity._1
      // println("actName ===> " + actName)
      val act =
        if(outAct_name_List.contains(actName)) {
          1
        }
        else -1

      var activity_List_byStd_temp2 = (actName, act)
      activity_List_byStd_temp2
    }
    var activity_List_byStd_temp3 = activity_List_byStd_temp1.map(_._2)

   activity_List_byStd = code_count_List ++ activity_List_byStd_temp3

   val act_list = activity_List_byStd.map(x => x.toString.toInt)
   // println("head" + head)
   // println("act_List" + act_list)
   act_tuples = act_tuples :+ (stdNO, act_list)
  }
  var act_df = act_tuples.toDF("STD_NO", "ACTING_COUNT")
  println("act complete!!")
  act_df
}

  val sbjt_res = sbjtFunc(spark, std_NO)
  val ncr_res = ncrFunc(spark, std_NO)
  val act_res = actFunc(spark, std_NO)

      //===========================================================================================================
      //===========================================================================================================
      //---------------------------------교과(sbjt_df), 비교과(ncr_df), 자율활동(act_df) join -------------------------------

    // def joinSim(spark:SparkSession, std_NO: Int) : DataFrame = {
    def joinSim(spark:SparkSession, sbjt_res: DataFrame, ncr_res: DataFrame, act_res: DataFrame) : DataFrame = {
      // val join_df_temp = sbjt_df.join(ncr_df, Seq("STD_NO"), "outer")
      // val join_df = join_df_temp.join(act_df, Seq("STD_NO"), "outer")
      val join_df_temp = sbjt_res.join(ncr_res, Seq("STD_NO"), "outer")
      val join_df = join_df_temp.join(act_res, Seq("STD_NO"), "outer")
      join_df.show
      // setMongoDF_USER_LIST(spark, join_df)
      //mongodb에 저장할 때 중복 제거해서 넣기
      MongoSpark.save(
        join_df.write
          .option("spark.mongodb.output.uri", "mongodb://127.0.0.1/cpmongo.USER_LIST_FOR_SIMILARITY")
          .mode("overwrite")
        )
          // mongodb collection 제거
          // db.USER_SIMILARITY.drop()
          //var userforSimilarity_table = getMongoDF(spark, userforSimilarityUri) //유사도 분석 팀이 생성한 테이블
          println("join complete!!")
          join_df
    } //joinSim end


  // var result = joinSim(spark, sbjt_res, ncr_res, act_res)

  // var stdNO = 20152611
  val join_res = joinSim(spark, sbjt_res, ncr_res, act_res)

  def calculateSim(spark:SparkSession, std_NO: Int) : DataFrame = {

    val schema_sim = StructType(
        StructField("STD_NO", StringType, true) ::
        StructField("SUBJECT_STAR", StringType, true) ::
        StructField("NCR_STAR", StringType, true) ::
        StructField("ACTING_COUNT", StringType, true) :: Nil)

    // var userforSimilarity_table = spark.createDataFrame(sc.emptyRDD[Row], schema_sim)

    val userforSimilarityUri = "USER_LIST_FOR_SIMILARITY" //유사도 분석 팀이 생성한 테이블
    val userforSimilarity_table = getMongoDF(spark, userforSimilarityUri) //유사도 분석 팀이 생성한 테이블

    var userforSimilarity_df = userforSimilarity_table.select(col("STD_NO"), col("SUBJECT_STAR"), col("NCR_STAR"), col("ACTING_COUNT"))
    userforSimilarity_df = userforSimilarity_df.drop("_id")

    if(userforSimilarity_df.count != 0){
      //질의자
      // var querySTD_NO = 20142820
      // var querySTD = userforSimilarity_df.filter(userforSimilarity_df("STD_NO").equalTo(s"${std_NO}")).drop("STD_NO")
      var querySTD = spark.createDataFrame(sc.emptyRDD[Row], schema_rdd)
      try{
        querySTD = userforSimilarity_df.filter(userforSimilarity_df("STD_NO").equalTo(s"${std_NO}")).drop("STD_NO")
      }
      catch{
        case e: NoSuchElementException => println("ERROR!")
        val empty_df = sqlContext.createDataFrame(sc.emptyRDD[Row], schema_rdd)
        empty_df
      }

      val exStr = "WrappedArray|\\(|\\)|\\]|\\["

      var querySTD_List = querySTD.collect.toList.mkString.replaceAll(exStr, "").split(",").map(x => (x.trim.toDouble * 10).toInt)
      var sbjt_star = querySTD.select(col("SUBJECT_STAR")).collect.toList.mkString.replaceAll(exStr, "").split(",").map(x=>(x.trim.toDouble * 10).toInt)
      var ncr_star = querySTD.select(col("NCR_STAR")).collect.toList.mkString.replaceAll(exStr, "").split(",").map(x=>(x.trim.toDouble * 10).toInt)
      var acting_count = querySTD.select(col("ACTING_COUNT")).collect.toList.mkString.replaceAll(exStr, "").split(",").map(x=>(x.trim.toDouble * 10 * -10).toInt)

      var w1, w2, w3 = 0.33 //가중치 값
      var a, b, r = 1 //알파, 베타, 감마

      object CosineSimilarity {
         def dotProduct(x: Array[Int], y: Array[Int]): Int = {
           (for((a, b) <- x zip y) yield a * b) sum
         }
         def magnitude(x: Array[Int]): Double = {
           math.sqrt(x map(i => i*i) sum)
         }
        def cosineSimilarity(x: Array[Int], y: Array[Int]): Double = {
          require(x.size == y.size)
          dotProduct(x, y)/(magnitude(x) * magnitude(y))
        }
      }

      var user_sim_tuples = Seq[(String, Double)]()
      var user_sim_tuples_sbjt = Seq[(String, Double)]()
      var user_sim_tuples_ncr = Seq[(String, Double)]()
      var user_sim_tuples_act = Seq[(String, Double)]()
      var stdNO_inDepart_List = userforSimilarity_df.select(col("STD_NO")).rdd.map(x => x(0)).collect().toList

      stdNO_inDepart_List.foreach(stdNO => {
        // var i = 0; //토탈 구해줄 떄 바꿀라고
        // println(stdNO)
        //유사사용자
        var std_inDepart = userforSimilarity_df.filter(userforSimilarity_df("STD_NO").equalTo(s"${stdNO}")).drop("STD_NO")
        var std_inDepart_List = std_inDepart.collect.toList.mkString.replaceAll(exStr, "").split(",").map(x => (x.trim.toDouble * 10).toInt)

        //교과
        var sbjt_star_ = std_inDepart.select(col("SUBJECT_STAR")).collect.toList.mkString.replaceAll(exStr, "").split(",").map(x=>(x.trim.toDouble * 10).toInt)
        //비교과
        var ncr_star_ = std_inDepart.select(col("NCR_STAR")).collect.toList.mkString.replaceAll(exStr, "").split(",").map(x=>(x.trim.toDouble * 10).toInt)
        //자율활동
        var acting_count_ = std_inDepart.select(col("ACTING_COUNT")).collect.toList.mkString.replaceAll(exStr, "").split(",").map(x=>(x.trim.toDouble * 10 * -10).toInt)
        // println(acting_count_)
        val sim = CosineSimilarity.cosineSimilarity(querySTD_List, std_inDepart_List)
        val sbjt_sim = CosineSimilarity.cosineSimilarity(sbjt_star, sbjt_star_)
        // println("sbjt_sim  : " + sbjt_sim)
        val ncr_sim = CosineSimilarity.cosineSimilarity(ncr_star, ncr_star_)
        // println("ncr_sim  : " + ncr_sim)
        val acting_sim = CosineSimilarity.cosineSimilarity(acting_count, acting_count_)
        // println("acting_sim  : " + acting_sim)
        //println(sim)

        user_sim_tuples_sbjt = user_sim_tuples_sbjt :+ (s"${stdNO}", sbjt_sim)
        user_sim_tuples_ncr = user_sim_tuples_ncr :+ (s"${stdNO}", ncr_sim)
        user_sim_tuples_act = user_sim_tuples_act :+ (s"${stdNO}", acting_sim)

        // var total_sim = (user_sim_tuples_sbjt(i)._2 * w1) + (user_sim_tuples_ncr(i)._2 * w2) + (user_sim_tuples_act(i)._2 * w3)
        var total_sim = (sbjt_sim * w1) + (ncr_sim * w2) + (acting_sim * w3)
        // println(total_sim)
        //
        // i += 1;

        //전체 유사도 구하기
        user_sim_tuples = user_sim_tuples :+ (s"${stdNO}", total_sim)
      })

      var user_sim_df = user_sim_tuples.toDF("STD_NO", "similarity")
      var user_sim_sbjt_df = user_sim_tuples_sbjt.toDF("STD_NO", "sbjt_similarity")
      var user_sim_ncr_df = user_sim_tuples_ncr.toDF("STD_NO", "ncr_similarity")
      var user_sim_acting_df = user_sim_tuples_act.toDF("STD_NO", "acting_similarity")

      var join_df_temp1 = user_sim_sbjt_df.join(user_sim_ncr_df, Seq("STD_NO"), "outer")
      var join_df_temp2 = join_df_temp1.join(user_sim_acting_df, Seq("STD_NO"), "outer")
      val user_sim_join_df = join_df_temp2.join(user_sim_df, Seq("STD_NO"), "outer")

      MongoSpark.save(
      user_sim_join_df.write
          .option("spark.mongodb.output.uri", "mongodb://127.0.0.1/cpmongo.USER_SIMILARITY")
          .mode("overwrite")
        )
      println("similarity complete!!")
      user_sim_join_df
    }
    else{
      val schema_totalsim = StructType(
          StructField("STD_NO", StringType, true) ::
          StructField("sbjt_similarity", StringType, true) ::
          StructField("ncr_similarity", StringType, true) ::
          StructField("acting_similarity", StringType, true) ::
          StructField("similarity", StringType, true) :: Nil)
      var user_sim_join_df = spark.createDataFrame(sc.emptyRDD[Row], schema_totalsim)
      MongoSpark.save(
      user_sim_join_df.write
          .option("spark.mongodb.output.uri", "mongodb://127.0.0.1/cpmongo.USER_SIMILARITY")
          .mode("overwrite")
        )
      println("similarity complete!!")
      user_sim_join_df
    }
  } //calculateSim end
  val calculateSim_res = calculateSim(spark, std_NO)
  calculateSim_res
  } //calSim end !!

  //====================================유사도팀(연희,소민) end==========================================

  //====================================추천결과팀(보경, 도웅) start==========================================
  //====================================추천결과팀(보경, 도웅) start==========================================

  // recResult함수(교과, 비교과, 자율활동 추천결과 생성)
  def recResult(std_NO:Int, user_trust_df:(DataFrame, DataFrame), user_sim_df:DataFrame, applyComlist:org.apache.spark.sql.DataFrame, searchComlist:org.apache.spark.sql.DataFrame, gStdComList:org.apache.spark.sql.DataFrame): (List[String], DataFrame) ={

    //유사도+신뢰도 DF생성 start
    val user_sim_DF = user_sim_df.drop("sbjt_similarity", "ncr_similarity", "acting_similarity")

    val user_Result_df = user_trust_df._1.join(user_sim_DF, Seq("STD_NO"), "outer")
    val user_Result_df_NaN = user_Result_df.na.fill(0.0, List("SIMILARITY")).na.fill(0.0, List("TRUST"))

    // 유사도, 신뢰도 보정상수(a,b), 가중치(w1,w2)
    val a = double2Double(1)
    val b = double2Double(1)
    val w1 = 0.5
    val w2 = 0.5

    // 유사도, 신뢰도 결합 및 연산 (결과값 : a*w1*sim + b*w2*tru)
    val res_ex_str = "\\[|\\]"
    val user_Result1 = user_Result_df_NaN.select("STD_NO", "SIMILARITY", "TRUST").collect.map(_.toString.replaceAll(res_ex_str, "")).map{ row =>
      val x = row.split(",")
      val stdNo = x(0)
      val sim = x(1).toDouble * a * w1
      val tru = x(2).toDouble * b * w2
      (stdNo, sim, tru, sim + tru)
    }.sortBy(x=> x._4).reverse

    //유사도+신뢰도 DF생성 end

    //유사도 + 신뢰도결합 최종 추천 졸업생 list
    val std_arr = user_Result1.map(x=>x._1).toSeq.toList.take(10)

    //-------------------- # # # 교과목수료 추천결과 생성 Start------------------------
    // 교과목수료 테이블 중 학번, 학과, 교과목번호, 과목명
    var clPassUri_table =  getMongoDF(spark, clPassUri)
    var clPassUri_DF = clPassUri_table.select(col("STD_NO"), col("SUST_CD_NM"), col("SBJT_KOR_NM"), col("SBJT_KEY_CD")).distinct.toDF

    // 추천졸업생의 수강과목 SBJT_KEY_CD list 생성
    val std_list = std_arr.map{ stdno =>
      val res = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${stdno}"))
      res
    }.map{ x =>
      val res = x.select("SBJT_KEY_CD").collect.map( x=> x.toString)
      res
    }.flatMap( x=> x)

    //한학생이 들은 수업리스트(질의자)
    var sbjtNM_by_stdNO = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${std_NO}")).select(col("SBJT_KEY_CD")).distinct
    var sbjtNM_by_stdNO_List = sbjtNM_by_stdNO.rdd.map(r=>r(0)).collect.toList.distinct.map(_.toString)

    //질의학생이 들은 과목 중복제거
    val exStr = "\\[|\\]"
    val t2 = std_list.map(x => x.replaceAll(exStr, ""))
    val std_filter = t2.diff(sbjtNM_by_stdNO_List)

    val std_sbjt_arr = std_filter.groupBy(x => x).mapValues(_.length).toList.sortBy(x => x._2).reverse

    clPassUri_table =  getMongoDF(spark, clPassUri) //교과목 수료

    //콘텐츠 신뢰도 DF->list변환(reduceDF_con1)
    import org.apache.spark.sql.functions.collect_list
    var reducedDF_con1 = user_trust_df._2.select("STAR_KEY_ID", "STAR_POINT").distinct()

    //콘텐츠 신뢰도 형변환
    val reducedDF_con2 = reducedDF_con1.groupBy("STAR_KEY_ID").agg(collect_list($"STAR_POINT").as("STAR_POINT")).rdd.map(row => (row(0).toString -> row(1).toString)).collect.map{ x =>
      val sbjt = x._1
      val starPoint = x._2.slice(13,x._2.length-1).toDouble
        (sbjt, starPoint)
      }

    //콘텐츠신뢰도 STAR_KEY_ID와 교과목수료 SBJT_KEY_CD일치 여부에 따라 값 생성
    val res_arr = std_sbjt_arr.map { x =>
      val sbjt = x._1
      val filtered_sbjt_arr = reducedDF_con2.filter( x=> x._1 == sbjt)
      val v = if(filtered_sbjt_arr.isEmpty) x._2
      else filtered_sbjt_arr(0)._2 * x._2
      (sbjt, v)
    }

    //교과목 결과값에 콘텐츠 신뢰도를 곱해 재 랭킹
    val PassUri_top5_list = res_arr.sortBy(x => x._2).reverse

    //-------------------- # # # 교과목수료 추천결과 생성 END------------------------

    //-------------------- # # # 비교과수료 추천결과 생성 Start----------------------
    //비교과 신청학생 테이블 중 학번, 비교과 프로그램 학생키아이디, 비교과 프로그램 학생키아이디, 과목명
    //NPS_STATE 코드 (NCR_T07_P00 : 대기신청, NCR_T07_P01 : 승인대기, NCR_T07_P02 :승인, NCR_T07_P03 : 학생취소,
    //NCR_T07_P04 : 관리자취소, NCR_T07_P05 : 이수, NCR_T07_P06 : 미이수, NCR_T07_P07 : 반려)
    val ncrStdInfoUri_table =  getMongoDF(spark, ncrStdInfoUri)
    var ncrInfoUri_DF = ncrStdInfoUri_table.select(col("NPS_STD_NO"), col("NPS_KEY_ID"), col("NPI_KEY_ID"), col("NPS_STATE")).distinct.toDF

    //졸업생중, 비교과 신청학생 List생성
    var ncrStdInfoUri_stdNO = ncrInfoUri_DF.filter(ncrInfoUri_DF("NPS_STD_NO").equalTo(s"${std_NO}")).select(col("NPI_KEY_ID")).distinct
    var ncrStdInfoUri_stdNO_List = ncrStdInfoUri_stdNO.rdd.map(r=>r(0)).collect.toList.distinct.map(_.toString)

    //비교과 신청 테이블에서 이수한 비교과 프로그램 mapping
    val ncrStdInfoUri_arr = std_arr.map{ stdno =>
    val res = ncrInfoUri_DF.filter(ncrInfoUri_DF("NPS_STD_NO").equalTo(s"${stdno}")).filter($"NPS_STATE" === "NCR_T07_P05")
    res
  }.map{ x =>
    val res = x.select("NPI_KEY_ID").collect.map( x=> x.toString)
    res
  }.flatMap( x=> x)

  //질의학생이 들은 과목 중복제거
    val ncr_replace = ncrStdInfoUri_arr.map(x => x.replaceAll(exStr, ""))
    val ncr_filter = ncr_replace.diff(ncrStdInfoUri_stdNO_List)

    val ncr_filter_arr = ncr_filter.groupBy(x => x).mapValues(_.length).toList.sortBy(x => x._2).reverse

    //콘텐츠신뢰도 STAR_KEY_ID와 비교과 NPI_KEY_ID 일치 여부에 따라 값 곱하기
    val res_arr2 = ncr_filter_arr.map { x =>
      val ncr = x._1
      val filtered_ncrStdInfoUri = reducedDF_con2.filter( x=> x._1 == ncr)
      val v = if(filtered_ncrStdInfoUri.isEmpty) x._2
      else filtered_ncrStdInfoUri(0)._2 * x._2
      (ncr, v)
    }
    //-------------------- # # # 비교과수료 추천결과 생성 End----------------------


    //-------------------- # # # 자율활동 추천결과 생성 Start------------------------
    //-------------------- # # # 자율활동 리스트 # # # ------------------------------
    //from.교외활동 CPS_OUT_ACTIVITY_MNG : 학번(OAM_STD_NO), 활동구분코드(OAM_TYPE_CD), 활동명(OAM_TITLE)
    //자격증(CD01) : 이름(OAM_TITLE) / ex. 토익800~900, FLEX 일본어 2A,  FLEX 일본어 1A,  FLEX 중국어 1A
    //어학(CD02) : 이름(OAM_TITLE)
    //봉사(CD03), 대외활동(CD04), 기관현장실습(CD05) : 활동구분코드(OAM_TYPE_CD)

    // 자율활동 테이블 중 학번, 학과, 교과목번호, 과목명
    var outActUri_DF = outActUri_table.select(col("OAM_STD_NO"), col("OAM_TYPE_CD"), col("OAM_TITLE"))

    var outAct_name_temp1 = outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${std_NO}")).select(col("OAM_STD_NO"), col("OAM_TITLE"), col("OAM_TYPE_CD")).distinct

    //자격증, 어학 List생성
    var outAct_name_temp2 = outAct_name_temp1.drop("OAM_STD_NO", "OAM_TYPE_CD").filter($"OAM_TYPE_CD" === "OAMTYPCD01" || $"OAM_TYPE_CD" ==="OAMTYPCD02").distinct
    var outAct_name_List = outAct_name_temp2.rdd.map(r=>r(0)).collect.toList

    //---------------------자율활동 추천 code list(자격증(CD01), 어학(CD02))----------------------
    val outActUri_CD01_arr = std_arr.map{ stdno =>
      val res_CD01 = outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${stdno}"))
      res_CD01
    }.map{ x =>
      val res_CD01 = x.select("OAM_TITLE").filter($"OAM_TYPE_CD" === "OAMTYPCD01").collect.toList.map( x=> x.toString)
      res_CD01
    }.flatMap( x=> x).groupBy(x => x).mapValues(_.length).toList.sortBy(x => x._2).reverse


    val outActUri_CD02_arr = std_arr.map{ stdno =>
      val res_CD02 = outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${stdno}"))
      res_CD02
    }.map{ x =>
      val res_CD02 = x.select("OAM_TITLE").filter($"OAM_TYPE_CD" ==="OAMTYPCD02").collect.toList.map( x=> x.toString)
      res_CD02
    }.flatMap( x=> x).groupBy(x => x).mapValues(_.length).toList.sortBy(x => x._2).reverse

    //---------------------자율활동 추천 code list(봉사(CD03), 대외활동(CD04), 기관현장실습(CD05))----------------------

    val outActUri_CD03 = std_arr.map{ stdno =>
      val res_CD03 = outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${stdno}"))
      res_CD03
    }.map{ x =>
      val res_CD03 = x.select("OAM_TYPE_CD").filter($"OAM_TYPE_CD" === "OAMTYPCD03").collect.toList
      res_CD03
    }.flatMap( x=> x).groupBy(x => x).mapValues(_.length).toList.sortBy(x => x._2).reverse

    val outActUri_CD03_result = outActUri_CD03.map(x => x._2)

    case class OAM_TYPE_CD_CD03(OAM_TYPE_CD: Int, count : Int)
    val outActUri_CD03_arr = outActUri_CD03_result.map{ row =>
      val avg = row / std_arr.length
      val res_CD03 = ("OAM_TYPE_CD03", avg)
      res_CD03
    }

    val outActUri_CD04 = std_arr.map{ stdno =>
      val res_CD04 = outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${stdno}"))
      res_CD04
    }.map{ x =>
      val res_CD04 = x.select("OAM_TYPE_CD").filter($"OAM_TYPE_CD" ==="OAMTYPCD04").collect.toList
      res_CD04
    }.flatMap( x=> x).groupBy(x => x).mapValues(_.length).toList.sortBy(x => x._2).reverse

    val outActUri_CD04_result = outActUri_CD04.map(x => x._2)

    case class OAM_TYPE_CD_CD04(OAM_TYPE_CD: Int, count : Int)
    val outActUri_CD04_arr = outActUri_CD04_result.map{ row =>
      val avg = row / std_arr.length
      val res_CD04 = ("OAM_TYPE_CD04", avg)
      res_CD04
    }

    val outActUri_CD05 = std_arr.map{ stdno =>
      val res_CD05 = outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${stdno}"))
      res_CD05
    }.map{ x =>
      val res_CD05 = x.select("OAM_TYPE_CD").filter($"OAM_TYPE_CD" ==="OAMTYPCD05").collect.toList
      res_CD05
    }.flatMap(x=> x).groupBy(x => x).mapValues(_.length).toList.sortBy(x => x._2).reverse

    val outActUri_CD05_result = outActUri_CD05.map(x => x._2)

    case class OAM_TYPE_CD_CD05(OAM_TYPE_CD: Int, count : Int)
    val outActUri_CD05_arr = outActUri_CD05_result.map{ row =>
      val avg = row / std_arr.length
      val res_CD05 = ("OAM_TYPE_CD05", avg)
      res_CD05
    }

    ////---------------------------------교과(sbjt_df), 비교과(ncr_df), 자율활동(act_df) join (Key값 필요) -------------------------------

    // 추천할 교과, 비교과, 자율활동 개수 정의
    val maxSize = 20
    var colSet = scala.collection.mutable.Set[String]()

    // forFM 성능평가를 위한 List생성
    val PassUri_top5_forFM = res_arr.sortBy(x => x._2).reverse.take(maxSize).zipWithIndex.map(x => x._1._1)

    //최종 추천결과 생성
    val PassUri_top5 = res_arr.sortBy(x => x._2).reverse.take(maxSize).zipWithIndex.map(x => (x._2 + 1, x._1._1)).toDF("Rank","SBJT_KEY_CD")
    val NCR_std_Info_top5 = res_arr2.sortBy(x => x._2).reverse.take(maxSize).zipWithIndex.map(x => (x._2 + 1, x._1._1)).toDF("Rank","NPI_KEY_ID")
    val CD01_top5 = outActUri_CD01_arr.take(maxSize).zipWithIndex.map(x => (x._2 + 1, x._1._1)).toDF("Rank","OAMTYPCD01")
    val CD02_top5 = outActUri_CD02_arr.take(maxSize).zipWithIndex.map(x => (x._2 + 1, x._1._1)).toDF("Rank","OAMTYPCD02")
    val CD03_avg = outActUri_CD03_arr.zipWithIndex.map(x => (x._2 + 1, x._1._2)).toDF("Rank", "OAMTYPCD03_AVG")
    val CD04_avg = outActUri_CD04_arr.zipWithIndex.map(x => (x._2 + 1, x._1._2)).toDF("Rank", "OAMTYPCD04_AVG")
    val CD05_avg = outActUri_CD05_arr.zipWithIndex.map(x => (x._2 + 1, x._1._2)).toDF("Rank", "OAMTYPCD05_AVG")

    //rankList 생성
    val rankList = Seq(NCR_std_Info_top5, CD01_top5, CD02_top5, CD03_avg, CD04_avg, CD05_avg)

    //최종적으로 join후 DF생성
    var Result_All = PassUri_top5
    rankList.foreach{DF =>
      Result_All = Result_All.join(DF, Seq("Rank"), "outer")
    }

    setMongoDF_result(spark, Result_All) //mongodb저장

    println(Result_All.sort("Rank").show)

    (PassUri_top5_forFM, Result_All)


  } //recResult end !!

  def evaluation(std_NO: Int, recommed_list:List[String]): Unit = {

    val clPassUri = "V_STD_CDP_PASSCURI" //교과목 수료(class pass)
    var clPassUri_DF = clPassUri_table.select(col("STD_NO"), col("SUST_CD_NM"), col("SBJT_KOR_NM"), col("SBJT_KEY_CD")).distinct.toDF

    val schema_string = "Similarity"
    val schema_rdd = StructType(schema_string.split(",").map(fieldName => StructField(fieldName, StringType, true)) )

    var departNM_by_stdNO = spark.createDataFrame(sc.emptyRDD[Row], schema_rdd)
    try{
      departNM_by_stdNO = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${std_NO}")).select(col("SUST_CD_NM")).distinct
    }
    catch{
      case e: NoSuchElementException => println("ERROR!")
      val empty_df = sqlContext.createDataFrame(sc.emptyRDD[Row], schema_rdd)
      empty_df
    }
    var departNM = departNM_by_stdNO.collect().map(_.getString(0)).mkString("")

    var sbjt_origin_temp1 = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}"))
    var sbjt_origin_temp2 = sbjt_origin_temp1.groupBy("SBJT_KEY_CD").count().orderBy($"count".desc)
    val sbjt_origin_temp3 = sbjt_origin_temp2.select("SBJT_KEY_CD").limit(20)
    val sbjt_origin_list_10 = sbjt_origin_temp3.rdd.map(r=>r(0)).collect.toList //<정답셋 리스트>


    var contain_count = 0
    recommed_list.foreach{ list =>
      if(sbjt_origin_list_10.contains(list)){
        contain_count = contain_count+1
      }
      contain_count
    }

    val precision = contain_count.toFloat/sbjt_origin_list_10.length
    val recall = contain_count.toFloat/recommed_list.length
    val f_measure = 2*((recall*precision)/(recall+precision))
    println("precision : " + precision)
    println("recall : " + recall)
    println("f_measure : " + f_measure)

    (precision, recall, f_measure)

    }


  object Main{
    def main(args:Array[String]):Unit={
      if(args.length==0 || args.length>1){
        println("1개의 학번을 인자로 입력해주세요. ex)Main.main(Array(\"학번\"))")
        sys.exit(0)
      }

      var std_no = args(0).toInt

      var i = INTEREST(std_no)
      var t = TRUST(i._1, i._2, i._3)
      var c_temp = calSim(spark, std_no)
      var c = c_temp.drop("sbjt_similarity", "ncr_similarity", "acting_similarity")
      var r =  recResult(std_no, t, c, i._1, i._2, i._3)

      var evaluation_test = evaluation(std_no, r._1)

      print(evaluation_test)

    }
  }

  // Main.main(Array("20152611"))
