package example

/**
 * @author xiafan
 *
 */

//空类
class Person {
  private var name = ""
  private var age: Int=1
  private var gender: Boolean=false
 
  def this(name: String, age: Int, gender: Boolean) {
    this()
    this.name = name
    this.age = age
    this.gender = gender
  }

  def print() = {
    println(name + "," + age + "," + gender)
  }
}

trait TLecturer extends Person {
  //虚方法，没有实现 
  def teach
}

trait Programmer extends Person {
  //提供实现 
  def writeProgram = { println("I’m writing a program. ") }

}

class ProgramTeacher extends Person with TLecturer with Programmer {
  //定义虚方法的实现
  def teach = { println("I’m teaching students about programming. ") }
}  