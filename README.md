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
