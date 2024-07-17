
package com.example

import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector
import org.apache.kafka.common.config.ConfigDef
import java.util.*

class MySourceConnector : SourceConnector() {

    private lateinit var configProps: Map<String, String>

    /**
     * 생명주기: 커넥터 시작
     * - props 미리 확인하고, validation 해볼 수 있음.
     * - 커넥터 설정을 초기화하고 필요한 리소스를 준비합니다.
     */
    override fun start(props: Map<String, String>) {

        configProps = props
    }

    /**
     * 커넥터의 실제 테스크 정의
     */
    override fun taskClass(): Class<out Task> {
        return MySourceTask::class.java
    }

    /**
     * 각 Task에 전달할 설정 생성
     * - 내부적으로 전달 받은 props 를 커스텀해서 config 를 만들 수 있음
     * - task.id 등을 커스텀 할 거면 여기서 설정할 수 있음.
     */
    override fun taskConfigs(maxTasks: Int): List<Map<String, String>> {
        return List(maxTasks) { configProps }
    }

    /**
     * 생명주기: 커넥터 종료될 때
     * - 모든 리소스를 해제하고 종료 작업을 수행합니다.
     */
    override fun stop() {

    }

    /**
     * 커스텀 커넥터 설정 파일 구현체 응답
     * - 각 설정의 유형, 중요도, 설명 등을 포함한 ConfigDef 객체를 반환합니다
     */
    override fun config(): ConfigDef {
        // 커넥터 설정 정의
        return ConfigDef()
                .define("source.topic", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Source Kafka Topic")
                .define("dest.topic", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Destination Kafka Topic")
    }

    /**
     * 커넥터의 버전 반환
     */
    override fun version(): String {
        return "1.0"
    }
}
