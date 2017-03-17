import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import examples.attribution.misc.Utils
import org.apache.spark.sql.SparkSession
import examples.attribution.EventProcessor
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull


class TestSuite extends FunSuite with BeforeAndAfter {
  
  var sc:SparkSession = _
  
  var ep:EventProcessor = _
     
  before{
     sc = Utils.getSession()
     ep = new EventProcessor(sc,this.getClass.getResource("/").getPath)
    
  }
  
  after{
    sc.stop()
  }
  
  test("Attributed Events Test 1: Should Fail: Event should not be in the list of attributed events"){
    
     val out =  ep.process()
     
     val attEvtData = out._3     
     
     val evts = attEvtData.values.reduce((x,y) => x.toList ++ y.toList).toList.filter { e => e.event_id == "253ea62f-0fd2-4b54-bb9b-238df34ff2f5"}    
 
     assert(!evts.isEmpty)
     
     
        
  }
  
  test("Attributed Events Test 2 : Should Pass: Event should be in the list of attributed events"){
    
     val out =  ep.process()
     
     val attEvtData = out._3     
     
     val evts = attEvtData.values.reduce((x,y) => x.toList ++ y.toList).toList.filter { e => e.event_id == "d79fbe33-ea94-4d38-bebc-05bbb9b770a3"}    
 
     assert(!evts.isEmpty)
     
     
        
  }
  
  
  
  
}