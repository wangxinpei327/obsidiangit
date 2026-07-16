1. ./build/mvn clean deploy -DskipTests -Papache-release,spark-provided,hive-provided,spark-4.1,scala-2.13
	报错，编译的是1.10.3版本，没有spark-4.1的tag，下载1.11.1版本
2. 编译2.13版本的提示mvn仓库找不到jar包，官方源没有2.13的jar包，将mvn package改为mvn deploy
3. 