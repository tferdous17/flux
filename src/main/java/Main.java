public class Main {
//    public producerRecordTest(){
//
//    }
    public static void main(String[] args) {
        String key = "Kyoshi";
        Integer value = 22;

        ProducerRecord<String, Integer> test1 = new ProducerRecord<String, Integer>("Age", 1, Long.valueOf(1412), key, value);
//        ProducerRecord<String, Integer> test2 = new ProducerRecord<String, Integer>("Age", 1, Long.valueOf(1412), key, value);
        ProducerRecord<String, Integer> test3 = new ProducerRecord<String, Integer>("Age", 1, key, value);
//        ProducerRecord<String, Integer> test4 = new ProducerRecord<String, Integer>("Age", 1, Long.valueOf(1412), key, value);
        ProducerRecord<String, Integer> test5 = new ProducerRecord<String, Integer>("Age", key, value);
        ProducerRecord<String, Integer> test6 = new ProducerRecord<String, Integer>("Age", value);


        System.out.println("TEST 1");
        System.out.println(test1 + "\n");

//        System.out.println("TEST 2");
//        System.out.println(test2 + "\n");

        System.out.println("TEST 3");
        System.out.println(test3 + "\n");

//        System.out.println("TEST 4");
//        System.out.println(test4 + "\n");

        System.out.println("TEST 5"); // FIX HERE
        System.out.println(test5 + "\n");

        System.out.println("TEST 6");
        System.out.println(test6 + "\n");

    }
}
