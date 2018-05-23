import java.util.concurrent.CountDownLatch;

public class BeanProp {
    private static class InnerClass {
        static {
            System.out.println("init inner");
        }

        static int x = 10;
    }


    private static void testInnerClass() {
        for (int i = 0; i < 2; i++) {
            if (i < 2) System.out.println(InnerClass.x);
        }
    }

    private static void testAnd() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Runnable runnable = new Runnable() {
            TestBeanProperty beanProp = new TestBeanProperty(null, null, 1);

            @Override
            public void run() {
                if (beanProp != null && beanProp.getColdData() != null && !beanProp.getColdData().isEmpty()) {
                    System.out.println("null.");
                }
                System.out.println("not null.");
            }
        };

        Thread[] t = new Thread[10];
        for (int i = 0; i < 10; ++i) {
            t[i] = new Thread(runnable);
            t[i].start();
        }

        countDownLatch.await();
    }

    public static void main(String[] args) {
        testInnerClass();
    }


}
