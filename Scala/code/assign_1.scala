class SName(id:Int,name:String,result:Boolean){
private var s_id = id
private var s_name = name
private var s_result = result
  
def this(id:Int,name:String)=this(id,name,true)
def this(id:Int,passed:Boolean)=this(id,"avinash",passed)
def this(name:String,passed:Boolean)=this(1,name,passed)
def this()=this(1,"avinash",true)
  
def getStudentId() = s_id
def getStudentName() = s_name
def getStudentResult() = s_result
  
def setStudentId_=(id:Int)= s_id=id
def setStudentName_=(name:String)= s_name=name
def setStudentResult_=(result:Boolean)= s_result=result
  
def statusReport() ={
var print_result:String = "passed"
if(s_result == false)
print_result = "failed";
println(s"Student with id $s_id whose name is $s_name has $print_result")
}
}
