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
