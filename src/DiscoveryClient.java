import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;

/**
 * Created by leo on 10/10/15.
 */
public class DiscoveryClient implements Runnable{


    @Override
    public void run() {
        startClient();
    }

    public void startClient(){

        while(true){

            System.out.println("***********Please enter the option************");
            System.out.println("1, Display Active Nodes :");


            Scanner scanner = new Scanner(System.in);

            int input =0;

            try{
                do{
                    System.out.println("Please enter the Number :");
                    while (!scanner.hasNextInt()){
                        scanner.next();
                        System.out.println("Please enter a valid number");
                    }
                    input = scanner.nextInt();
                }while (input<1 || input>1);
            }catch (Exception e){
                System.out.println("Enter a valid Number");
            }


            switch (input) {
                case 1:
                    Set<String> allDiscovery = Discovery.nodeDetails.keySet();
                    Iterator<String> itr = allDiscovery.iterator();

                    while(itr.hasNext()){

                        String tempval = itr.next();
                        System.out.println(Discovery.nodeDetails.get(tempval).getIdentifier()+" "+Discovery.nodeDetails.get(tempval).getIpAddress()+"\n");

                    }

            }




        }


    }

}
