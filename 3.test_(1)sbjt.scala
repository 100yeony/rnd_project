//-------------------- # # # 교과목 리스트 # # # --------------------------------
//--------------------from. 교과목수료 테이블 : 학과명, 학번, 학점----------------------
//<학과 DataFrame> : departDF / 전체 학과의 모든 학생
//###학과 별 학생 번호 보기 ###

 // 201028406
 // 201028409
 // 201028407
 // 201028410
 // 201028411
//-------------------------------------------------------
//studentDF_test 중 한명의 학번
//컴퓨터공학과 학생으로 테스트함
// 1. 학생이 질의 -> 학번으로 질의함
// 2-1. 학번으로 그 학생이 무슨 과인지 질의 => 질의 한 후 String으로 학과를 저장함
// 2-2. 학번의로 그 과의 소속 학생들을 리스트로 저장
// 3-1 학과 이름으로 과에 있는 학생들이 수강한 교과목을 리스트로 저장
// 3-2 학과 이름으로 과에서 개설된 교과목을 리스트로 저장
//
var std_NO = 20142820
// 컴공과 데이터 목록 |20142820||20142932| |20152611|
//--------------------from. 교과목수료 테이블 V_STD_CDP_SUBJECT : 학과이름, 학번, 수업명, 교과목코드-------------------------
// var studentNO = students_in_departNM.filter(students_in_departNM("STD_NO").equalTo(s"${std_NO}"))

// 2-1. 학과
var clPassUri_DF = clPassUri_table.select(col("STD_NO"), col("SUST_CD_NM"), col("SBJT_KOR_NM"), col("SBJT_KEY_CD")).distinct.toDF
var departNM_by_stdNO = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${std_NO}")).select(col("SUST_CD_NM")).distinct
var departNM = departNM_by_stdNO.collect().map(_.getString(0)).mkString("")

// 2-2. 학과 학생 리스트
var stdNO_in_departNM = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}")).select(col("STD_NO"))
var stdNO_in_departNM_List = stdNO_in_departNM.rdd.map(r=>r(0)).collect.toList.distinct

// 3-1. 학생의 수업 리스트
var sbjtNM_by_stdNO = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${std_NO}")).select(col("SBJT_KOR_NM"))
var sbjtNM_by_stdNO_List = sbjtNM_by_stdNO.rdd.map(r=>r(0)).collect.toList.distinct


// 3-2. 학과의 수업 리스트
// 컴퓨터공학과에서 개설된 수업명을 리스트로 생성 : 과목명이 1452개 -> distinct 지정distinct하게 자름 => 108개
var sbjtNM_in_departNM = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}")).select("SBJT_KOR_NM", "STD_NO")
var sbjtNM_in_departNM_List = sbjtNM_in_departNM.rdd.map(r=>r(0)).collect.toList.distinct


/*
// --------------------------------------------별점 추가하기---------------------------------------------
// 컴퓨터 공학과 학생 4명에 대해 별점 테이블 데이터 추가함 : |20142820||20142932| |20152611| |20152615|
// 일단 지금은 이 학생들에 대핸 비교과 활동에 대한 별점을 추가했음
// 이 학생들의 교과 리스트를 가져와서 (Key) 그 Key에 해당하는 별점을 mongodb에 추가해줌

// |20142820||20142932| |20152611| /////////////// |20152615|
// 과목마다 같은 과목이여도 코드가 다를 수 있으므로 한번에 select를 하여 code를 가져와야함
var std_NO1 = 20142820 (6개)
var sbjtNM_by_stdNO1 = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${std_NO1}")).select(col("SBJT_KOR_NM"), col("SBJT_KEY_CD")).show
// 네트워크프로그래밍, 시스템분석설계, 인공지능, 네트워크보안, 정보기술세미나, 서버구축및관리
// AAM00351, AAM00341, AAM00361, AAM00331, AAM00371, AAM00121

var std_NO2 = 20142932 (7개)
var sbjtNM_by_stdNO2 = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${std_NO2}")).select(col("SBJT_KOR_NM"), col("SBJT_KEY_CD")).show
// 시스템분석설계, 네트워크보안, 일본어회화Ⅰ, 네트워크프로그래밍, "MOSⅡ ", 인공지능, 정보기술세미나
// AAM00341, AAM00331, TAA02231, AAM00351, TAA04151, AAM00361, AAM00371

var std_NO3 = 20152611 (7개)
var sbjtNM_by_stdNO3 = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${std_NO3}")).select(col("SBJT_KOR_NM"), col("SBJT_KEY_CD")).show
// 컴퓨터구조, 취업연계현장실습Ⅰ, 인간중심감성공학의이해, 운영체제, 자바프로그래밍, 데이터베이스응용, 정보통신
// AAM00231, COM00051, BCD00017, AAM00191, AAM00241, AAM00201, AAM00211

// --------------------------------------------별점 추가하기 끝---------------------------------------------
*/

// from. 교과/비교과용 별점 테이블(CPS_STAR_POINT) : 학번(STD_NO), 비교과id(STAR_KEY_ID), 별점(STAR_POINT), 타입(TYPE)
var cpsStarUri_DF = cpsStarUri_table.select(col("STD_NO"), col("STAR_KEY_ID"), col("STAR_POINT"), col("TYPE"))

// 비교과 별점 => 별점 테이블에서 "TYPE"이 "N" :: ex) NCR000000000677
var cpsStarUri_ncr_DF = cpsStarUri_DF.filter(cpsStarUri_DF("TYPE").equalTo("N"))
// 교과 별점 => 별점 테이블에서 "TYPE"이 "C" :: ex) BAQ00028
var cpsStarUri_sbjt_DF = cpsStarUri_DF.filter(cpsStarUri_DF("TYPE").equalTo("C"))

// 학번 별 과목 ID, 별점을 가져감
var getStar_by_stdNO = cpsStarUri_sbjt_DF.filter(cpsStarUri_sbjt_DF("STD_NO").equalTo(s"${std_NO}")).show





//-----------------------------------------------학생 한명----------------------------------------------------
var student_have_sbjt_temp1 = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}"))
var student_have_sbjt_temp2 = student_have_sbjt_temp1.filter(student_have_sbjt_temp1("STD_NO").equalTo(s"${std_NO}"))
var student_have_sbjt_temp3 = student_have_sbjt_temp2.select(col("SBJT_KOR_NM"))

//@@@ 컴퓨터공학과의 학생 한명이 수강한 수업 리스트를 생성
var student_have_sbjt_List = student_have_sbjt_temp3.select("SBJT_KOR_NM").rdd.map(r=>r(0)).collect.toList.distinct

val isListened_List_temp1 = sbjtNM_List.map(x => (x, 0)).map{ record =>
  //x : record_1
  //0 : record_2
  //isListend면 1로 바뀜
  val name = record._1
  val isListened =
    if(student_have_sbjt_List.contains(name)) {
      1
    }
    else 0
  val isListened_List_temp2 = (name, isListened)
  print(isListened_List_temp2)
  //리턴하려면 이름을 쳐야 함
  //최종적으로 isListened_List_temp1 = isListened_List_temp2 값이 담기는 것 !!
  isListened_List_temp2
}
val isListened_List = isListened_List_temp1.map(_._2)
//-----------------------------------------------=----------------------------------------------------------------


//==================================================일단 계산ㅇ ㅣ너무 느리니까 다른 방법으로,,=======================================
var sbjt_tuples = Seq[(String, String)]()

// 학번이 1452개 -> distinct 지정 -> 223 명
var stdNO_in_departNM = clPassUri_DF.filter(clPassUri_DF("SUST_CD_NM").equalTo(s"${departNM}")).select(col("STD_NO")).rdd.map(r=>r(0)).collect.toList.distinct

val arr01 = Array(20142820, 20142932, 20152611)
var arr02 = arr01.toList.map(_.toString)

arr02.foreach{ stdNO =>

  // 학생별로 (stdNO) 들은 교과목 테이블
  var student_have_sbjt_temp1 = sbjtNM_in_departNM.filter(sbjtNM_in_departNM("STD_NO").equalTo(s"${stdNO}"))
  var student_have_sbjt_temp2 = student_have_sbjt_temp1.select(col("SBJT_KOR_NM"))
  student_have_sbjt_temp2.show

  // 학과 전체 교과목 리스트를 순회 (교과, 0)으로 만들어놓음
  val isListened_List_temp1 = sbjtNM_List.map(x => (x, 0)).map{ record =>
    // println(s"stdNO : ${stdNO} ============= sbjtNM : ${record}")
    //
    //@@@ 컴퓨터공학과의 학생 한명이 수강한 수업 리스트를 생성
    var student_have_sbjt_List = student_have_sbjt_temp2.select("SBJT_KOR_NM").rdd.map(r=>r(0)).collect.toList.distinct
    // println(s"student_have_sbjt_List--${student_have_sbjt_List}")
    //x : record_1
    //0 : record_2
    //isListend면 1로 바뀜
    val name = record._1

    val isListened =
      if(student_have_sbjt_List.contains(name)) {
        1
      }
      else 0
    val isListened_List_temp2 = (name, isListened)
    // print(isListened_List_temp2)
    //리턴하려면 이름을 쳐야 함
    //최종적으로 isListened_List_temp1 = isListened_List_temp2 값이 담기는 것 !!
    isListened_List_temp2
  }
  val isListened_List = isListened_List_temp1.map(_._2).toString
  println(isListened_List)

  sbjt_tuples = sbjt_tuples :+ (stdNO, isListened_List)
}

var sbjt_df = sbjt_tuples.toDF("STD_NO", "SUBJECT")