class PrimeAlgorithm:

    # implement a prime checking algorithm here
  
    def is_prime(n: int) -> bool:
        if n <= 1:
            return False
        if n <= 3:
            return True
        if n % 2 == 0 or n % 3 == 0:
            return False

        i = 5
        while i * i <= n:
            if n % i == 0 or n % (i + 2) == 0:
                return False
            i += 6
        return True
'''
test_numbers = [1, 2, 3, 4, 5, 9, 11, 13, 25, 29, 97, 100]
result=[]
for n in test_numbers:
    result.append(PrimeAlgorithm.is_prime(n)) 

print(result)
'''