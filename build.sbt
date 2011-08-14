name := "meadow"
 
scalaVersion := "2.8.1"
 
resolvers += "Java.net Maven2 Repository" at "http://download.java.net/maven/2/"

// Customize any further dependencies as desired
libraryDependencies ++= Seq(
  "net.liftweb"   %% "lift-common"        % "2.4-M3" % "compile->default" withSources(),
  "org.mongodb"    % "mongo-java-driver"  % "2.5.3"                       withSources(),
  "junit"          % "junit"              % "4.8.2"  % "test"             withSources(),
  "com.novocode"   % "junit-interface"    % "0.6"    % "test",
  "joda-time"      % "joda-time"          % "1.6"                         withSources()
)
