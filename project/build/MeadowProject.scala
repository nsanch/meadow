import sbt._
import sbt.Process._

class MeadowProject(info: ProjectInfo) extends DefaultProject(info) {
  override def disableCrossPaths = true
  
  val ibiblioRepo = "java Net" at "http://mirrors.ibiblio.org/pub/mirrors/maven2/"
  
  
  val liftVer = "2.4-M2_fs_a"
  val liftCommon = "net.liftweb"       %% "lift-common"        % liftVer     % "compile" withSources()
  val mongo      = "org.mongodb"        % "mongo-java-driver"  % "2.5.3"                 withSources()
  val junit      = "junit"              %  "junit"             % "4.8.2"     % "test"    withSources()
  val junitInterface = "com.novocode"   %  "junit-interface"     % "0.6"   % "test"

  // ivy.xml generation
  def ivyPublishConfiguration = new DefaultPublishConfiguration("local", "release", true) {
    override def configurations = Some(List(Configurations.Compile,
                                            Configurations.Javadoc,
                                            Configurations.Optional,
                                            Configurations.Provided,
                                            Configurations.Runtime,
                                            Configurations.Sources,
                                            Configurations.System,
                                            Configurations.Test))
  }
}
