class SName(s_id:Int,s_name:String,s_res:Boolean){
private var _id=s_id
private var _name=s_name
private var _res=s_res
  
def id_=(x:Int) = _id = x
def name_=(y:String) = _name = y
def res_=(z:Boolean) = _res = z
  
def id = _id
def name = _name
def res = _res
  
def this() = this(1,"avinash",true)
def this(x:Int,y:String)=this(x,y,true)
def this(y:String,z:Boolean) = this(1,y,z)

  
def statusReport() ={
var print_result:String = "passed"
if(_res == false){
print_result = "failed"
}
println(s"Student with id $id whose name is $name has $print_result")
}
}
