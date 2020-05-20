//============================유사도(연희,소민)start=======================================

  /**
  //======================================================================================================
  //  calSim(유사도 전체 함수) : 유사도 계산을 위한 sbjtFunc, ncrFunc, actFunc, joinSim, calculateSim 함수가 정의되어 있다.
  //======================================================================================================
  **/
  def calSim (spark:SparkSession, std_NO: Int) : DataFrame = {
    //======================================================================================================
    // calSim 함수 전체에서 사용하는 변수
    //======================================================================================================
    var clPassUri_DF = clPassUri_table.select(col("STD_NO"), col("SUST_CD_NM"), col("SBJT_KOR_NM"), col("SBJT_KEY_CD")).distinct.toDF
    var cpsStarUri_DF = cpsStarUri_table.select(col("STD_NO"), col("STAR_KEY_ID"), col("STAR_POINT"), col("TYPE"))
    var cpsStarUri_DF_ncr = cpsStarUri_table.select(col("STD_NO"), col("STAR_KEY_ID").as("NPI_KEY_ID"), col("STAR_POINT"), col("TYPE"))
    var ncrInfoUri_DF = ncrInfoUri_table.select(col("NPI_KEY_ID").as("NPI_KEY_ID_NCR"), col("NPI_AREA_SUB_CD"))
    var outActUri_DF = outActUri_table.select(col("OAM_STD_NO"), col("OAM_TYPE_CD"), col("OAM_TITLE"))

    /**
    질의자 학번이 들어오면 교과목 수료 테이블에서 해당 학번의 학과를 찾아 departNM에 저장한다.
    try-catch 문을 사용하여 학번이 존재하지 않는 경우 예외처리한다.
    **/
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

    /**
    stdNO_in_departNM_sbjt변수에 질의자의 학과에 속한 학생들의 학번을 저장한다.
    **/
    var stdNO_in_departNM_sbjt = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}")).select(col("STD_NO")).distinct
    // var stdNO_in_departNM_List = stdNO_in_departNM_sbjt.rdd.map(r=>r(0)).collect.toList.slice(0, 100).map(_.toString)
    var stdNO_in_departNM_List = stdNO_in_departNM_sbjt.rdd.map(r=>r(0)).collect.toList.map(_.toString)

    /**
    sbjtCD_in_departNM 변수에 질의자의 학과에 속한 학생들이 수강한 교과목 코드를 저장한다.
    **/
    var sbjtCD_in_departNM = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}")).select("SBJT_KEY_CD", "STD_NO")
    var sbjtCD_in_departNM_List = sbjtCD_in_departNM.rdd.map(r=>r(0).toString).collect.toList.distinct.sorted
    var sbjtNM_by_stdNO = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${std_NO}")).select(col("SBJT_KEY_CD"))
    var sbjtNM_by_stdNO_List = sbjtNM_by_stdNO.rdd.map(r=>r(0)).collect.toList.distinct

    /**
    비교과 별점 => 별점 테이블에서 "TYPE"이 "N" :: ex) NCR000000000677
    var cpsStarUri_DF_ncr_typeN = cpsStarUri_DF_ncr.filter(cpsStarUri_DF_ncr("TYPE").equalTo("N"))
    교과 별점 => 별점 테이블에서 "TYPE"이 "C" :: ex) BAQ00028
    **/
    var cpsStarUri_DF_ncr_typeN = cpsStarUri_DF_ncr.filter(cpsStarUri_DF_ncr("TYPE").equalTo("N"))

    /**
    //======================================================================================================
    //  sbjtFunc(교과목 함수) : 학과 전체 학생에 대한 교과목 수료 리스트 생성 함수
    //======================================================================================================
        from. 교과목수료 테이블 : 학과명, 학번, 학점
        질의자와 유사한 교과를 들은 학생을 찾기 위해 학과의 모든 학생에 대해 교과목 유사도 리스트 생성

        교과 리스트에 넣는 값의 유형은 다음과 같다.
        - 별점
        - 값이 없을 때 -1 (0점을 주지 않은 이유는 학생이 부여한 별점 점수가 0일 경우와 구분하기 위함)

        ex)
        컴퓨터공학과에서 개설된 교과목 리스트 = List[a, b, c, d, e] 일때, 모든 학번에 대하여
        학생 '가'가 수강한 교과목 리스트 = List[a, b],
        학생 '나'가 수강한 교과목 리스트 = List[c],
        질의자 학생 '다'가 수강한 교과목 리스트 = List[a, b, c]

        sbjtFunc함수를 실행한 후 결과는
        학생 '가' = [4, 3, -1, -1, -1],
        학생 '나' = [-1, -1, 3, -1, -1],
        질의자 학생 '다' = [2, 7, 5, -1, -1]
        으로 출력되기 때문에 학생 '다'와 유사한 학생은 '가'가 된다.
    **/

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

    /**
     for문을 돌면서 학과의 모든 학생에 대해 교과 리스트를 생성한다.
    **/
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

     /**
      학생이 수강했는데 별점을 부여하지 않은 교과목 코드 리스트를 생성한다.
     **/
      var not_rated = sbjtNM_by_stdNO_List filterNot getStar_by_stdNO.contains
      // println(not_rated)
      // stdNo_List_byMap_sbjt

      /**
       Map형태의 stdNo_List_byMap_sbjt에 포함된 학번에 대해서만 다음 과정을 수행한다.
      **/
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

    /**
    //======================================================================================================
    //  ncrFunc(비교과 함수) : 학과 전체 학생에 대한 비교과목 수료 리스트 생성 함수
    //======================================================================================================
        from. 교과/비교과용 별점 테이블(CPS_STAR_POINT) : 학번(STD_NO), 비교과id(STAR_KEY_ID), 별점(STAR_POINT), 타입(TYPE)
        from. 비교과 관련 테이블(CPS_NCR_PROGRAM_INFO) : 비교과id(NPI_KEY_ID), 중분류(NPI_AREA_SUB_CD)

        비교과는 특강 위주이기 때문에 교과목 처럼 학생 별로 일치하는 경우가 많지 않다.
        따라서 비교과목의 상위 분류(중분류코드)를 사용한다.
        그리고 해당 중분류에 포함된 비교과의 평균을 계산하여 학생 별 유사도를 계산한다.

        비교과 아이디(학번, 비교과id 사용 from.교과/비교과별점테이블)로 중분류 가져오기(비교과id, 중분류 from.비교과 관련 테이블)
        학과 별 학생들이 수강한 비교과의 중분류 list 로 포맷 잡고 : 학과 - 학번 돌면서 list 만들고 , 중분류로 바꿔주기
        학생 한명이 수강한 비교과 list -> 별점 가져오기(from. 교과/비교과 별점 테이블) -> 중분류 가져오기 -> 중분류 별 별점 avg 계산

        비교과 리스트에 넣는 값의 유형은 다음과 같다.
        - 비교과의 중분류 코드 별 별점의 평균
        - 값이 없을 때 -1 (0점을 주지 않은 이유는 학생이 부여한 별점 점수가 0일 경우와 구분하기 위함)
    **/
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
      val tmp2 = ncrInfoUri_DF
      val star_subcd_joinDF = cpsStarUri_DF_ncr_typeN.join(ncrInfoUri_DF, cpsStarUri_DF_ncr_typeN("NPI_KEY_ID") === ncrInfoUri_DF("NPI_KEY_ID_NCR"), "left_outer").drop("NPI_KEY_ID_NCR")

      stdNO_in_departNM_List.foreach { stdNO =>
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
          subcd_star_byDepart_Map+=(subcd_star_record)

          subcd_byStd_DF = star_subcd_DF.select(col("NPI_AREA_SUB_CD"))
          val subcd_byDepart_temp = subcd_byStd_DF.map{ row =>
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

    /**
    //======================================================================================================
    //  actFunc(자율활동 함수) : 학과 전체 학생에 대한 자율활동 수료 리스트 생성 함수
    //======================================================================================================
        from.교외활동 CPS_OUT_ACTIVITY_MNG : 학번(OAM_STD_NO), 활동구분코드(OAM_TYPE_CD), 활동명(OAM_TITLE)

        자율활동은 학생이 입력하는 이름이 제각각 다를 수 있어서 이름으로 구분해도 되는 경우, 그렇지 않은 경우로 나눠서 처리한다.
        자율활동구분코드(OAM_TYPE_CD) 5개는 다음과 같다.
        자격증(CD01), 어학(CD02), 봉사(CD03), 대외활동(CD04), 기관현장실습(CD05)

        자격증과 어학은 이름(OAM_TITLE)을 이용해 구분하여 자율활동 리스트에 넣는다.
        봉사, 대외활동, 기관현장실습은 활동구분코드(OAM_TYPE_CD)를 사용해 몇 번 참여하였는지 횟수를 카운트한다.

        자율활동 리스트에 넣는 값의 유형은 다음과 같다.
        - 봉사, 대외활동, 기관현장실습 에 대한 횟수 카운트
        - 자격증, 어학의 수강 여부
    **/
    def actFunc(spark:SparkSession, std_NO: Int) : DataFrame = {
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
      val outAct_code = outAct_code_temp2.collect

      for(i<-0 until outAct_code.size){
        maps(outAct_code(i)(0).toString) = outAct_code(i)(1).toString.toInt
      }
      val maps_ = maps.toSeq.sortBy(_._1).toMap
      val code_count_List = maps_.values.toList

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
       act_tuples = act_tuples :+ (stdNO, act_list)
      }
      var act_df = act_tuples.toDF("STD_NO", "ACTING_COUNT")
      println("act complete!!")
      act_df
    }

    /**
     joinSim함수에서는 sbjtFunc, ncrFunc, actFunc 함수에서 각각 생성한 데이터 프레임을 사용한다.
    **/
    val sbjt_res = sbjtFunc(spark, std_NO)
    val ncr_res = ncrFunc(spark, std_NO)
    val act_res = actFunc(spark, std_NO)

    /**
    //======================================================================================================
    //  joinSim(조인 함수) : 교과, 비교과, 자율활동을 조인하는 함수
    //======================================================================================================
      유사도 계산을 위해 교과, 비교과, 자율활동 데이터프레임을 조인하여 하나로 합친 뒤 DB의 USER_LIST_FOR_SIMILARITY 컬렉션에 저장한다.
    **/
    def joinSim(spark:SparkSession, sbjt_res: DataFrame, ncr_res: DataFrame, act_res: DataFrame) : DataFrame = {
      val join_df_temp = sbjt_res.join(ncr_res, Seq("STD_NO"), "outer")
      val join_df = join_df_temp.join(act_res, Seq("STD_NO"), "outer")
      join_df.show
      MongoSpark.save(
        join_df.write
          .option("spark.mongodb.output.uri", "mongodb://127.0.0.1/cpmongo.USER_LIST_FOR_SIMILARITY")
          .mode("overwrite")
        )
          println("join complete!!")
          join_df
    } //joinSim end

    /**
     calculateSim 함수에서는 joinSim함수에서 저장한 USER_LIST_FOR_SIMILARITY를 사용한다.
    **/
    val join_res = joinSim(spark, sbjt_res, ncr_res, act_res)

    /**
    //======================================================================================================
    //  calculateSim(유사도 계산 함수) : 질의자가 속한 학과의 모든 학생들과의 유사도를 계산한 뒤 USER_SIMILARITY에 저장한다.
    //======================================================================================================
    **/
    def calculateSim(spark:SparkSession, std_NO: Int) : DataFrame = {
      val schema_sim = StructType(
          StructField("STD_NO", StringType, true) ::
          StructField("SUBJECT_STAR", StringType, true) ::
          StructField("NCR_STAR", StringType, true) ::
          StructField("ACTING_COUNT", StringType, true) :: Nil)

      val userforSimilarityUri = "USER_LIST_FOR_SIMILARITY" //유사도 분석 팀이 생성한 테이블
      val userforSimilarity_table = getMongoDF(spark, userforSimilarityUri) //유사도 분석 팀이 생성한 테이블

      var userforSimilarity_df = userforSimilarity_table.select(col("STD_NO"), col("SUBJECT_STAR"), col("NCR_STAR"), col("ACTING_COUNT"))
      userforSimilarity_df = userforSimilarity_df.drop("_id")

      /**
      if문을 사용하여 최초 실행 시 DB에 저장된 데이터(USER_LIST_FOR_SIMILARITY)가 없을 때 예외처리한다.
      **/
      if(userforSimilarity_df.count != 0){
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

        /**
        유사도 비교를 위해 코사인 유사도 함수를 사용한다.
        **/
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
