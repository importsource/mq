# mq
An open source message broker written in Java 

###首先需要继承`AbstractThread`，实现定制化的消费者逻辑

```java
/**
 * 具体线程实现。消息怎么处理。你可以继承抽象类来定制一个处理逻辑
 * @author Hezf
 *
 */
public class DBAccessThread extends AbstractThread {
	public DBAccessThread(String msg) {
		this.msg = msg;
	}
	
	public DBAccessThread() {
		super();
	}

	@Override
	public void doIt() {
		//消费消息
		System.out.println("Added the message: " + msg + " into the Database");

	}

}

```

### 然后实现生产者逻辑：
```java
/**
 * 消息输送前端。客户端请求发送的api实现
 * @author Hezf
 *
 */
public class TestDriver {
	public static void main(String[] args) {
		//线程池管理者。然后加入消息
		ThreadPoolManager tpm = ThreadPoolManager.newInstance("com.importsource.mq.DBAccessThread");
		for (int i = 0; i < 5000; i++) {
			tpm.addMsg(String.valueOf(i));
		}
	}
}
```

###maven
```xml
<dependency>
    <groupId>com.importsource.mq</groupId>
    <artifactId>importsource-mq</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```
