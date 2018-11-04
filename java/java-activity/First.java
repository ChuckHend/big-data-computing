import java.util.*;

public class First{
    public static void main(String[] args){
        Scanner input = new Scanner(System.in);

        System.out.print("Enter first number: ");
        int value1 = input.nextInt();
        //ensure the value read in is non-negative
        while(value1 < 0){
            System.out.print("Enter in a positive number: ");
            value1 = input.nextInt();
        }
        System.out.print("Enter second number: ");
        int value2 = input.nextInt();
        //ensure the value read in is non-negative
        while(value2 < 0){
            System.out.print("Enter in a positive number: ");
            value2 = input.nextInt();
        }

        //call the prime func
        printPrime(value1, value2);
    }

    public static void printPrime(int first, int second){

        //assume the following, but test and make correction if needed
        int min = first;
        int max = second;
        if(first>second){
            min=second;
            max=first;
        }

        int numPrimes = 0;
        ArrayList<Integer> primes = new ArrayList<>();
        //check if there are primes, add them to the array
        for(int i=min+1; i<max; i++){

            if(isPrime(i)){
                primes.add(i);
                numPrimes++;
            }//end isPrime
        }//end iterate inputs

        //print No primes if there are none
        if(numPrimes==0){
            System.out.println("No Primes");
        }
        else{
            for(int prime : primes){
            System.out.print(prime + " ");
            }
        }
    }

    public static boolean isPrime(int value){
        //1 is not prime
        if(value==1){
            return false;
        }
        int limit = value/2;
        //if divisible by anything up to half its value,
        // then it is not prime
        for(int i = 2; i <= limit; ++i){
            if(value % i == 0){
                return false;
            }
        }
        return true;
    }
}
