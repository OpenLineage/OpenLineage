plugins {
    id("java-library")
    id("io.openlineage.spark-ng")
}

sparkBuilds {
    
}

repositories {
    mavenCentral()
}

dependencies {
    compileOnly("org.apache.spark:spark-sql_2.12:3.5.0")
}

tasks.register("print") {
    group = "aux"
    description = "Prints all configurations"
    doLast {
        this.project.configurations.forEach {
            println(it.name)
        }
    }
}
