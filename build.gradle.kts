import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    kotlin("jvm") version "1.8.21"
    application
    id("com.github.johnrengelman.shadow") version "7.1.2"
}

group = "com.github.cookieshake.flinktest"
version = "1.0-SNAPSHOT"

val flinkVersion by extra {"1.17.0"}

repositories {
    mavenCentral()
}

val flinkShadowJar: Configuration by configurations.creating {
    exclude(group="org.apache.flink", module="force-shading")
    exclude(group="com.google.code.findbugs", module="jsr305")
    exclude(group="org.slf4j")
    exclude(group="org.apache.logging.log4j")
}

dependencies {
    implementation("org.apache.flink", "flink-streaming-java", flinkVersion)
    implementation("org.apache.flink", "flink-clients", flinkVersion)
    implementation("org.apache.flink", "flink-queryable-state-runtime", flinkVersion)
    implementation("org.apache.flink", "flink-queryable-state-client-java", flinkVersion)
    implementation("com.github.ajalt.mordant", "mordant", "2.0.0-beta13")
    testImplementation(kotlin("test"))
}

sourceSets {
    main {
        compileClasspath += flinkShadowJar
        runtimeClasspath += flinkShadowJar
    }
    test {
        compileClasspath += flinkShadowJar
        runtimeClasspath += flinkShadowJar
    }
}

tasks.test {
    useJUnitPlatform()
}

tasks.named<ShadowJar>("shadowJar") {
    isZip64 = true
}

kotlin {
    jvmToolchain(11)
}

application {
    mainClass.set("MainKt")
}