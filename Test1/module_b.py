from module_a import ClassA

class ClassB:
    def __init__(self, name):
        self.class_a_instance = ClassA(name)
    
    def greet(self):
        return f"ClassB says: {self.class_a_instance.greet()}"
