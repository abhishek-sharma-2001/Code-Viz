from module_a import ClassA
from module_b import ClassB

def main():
    obj_a = ClassA("Alice")
    print(obj_a.greet())
    
    obj_b = ClassB("Bob")
    print(obj_b.greet())

if __name__ == "__main__":
    main()
