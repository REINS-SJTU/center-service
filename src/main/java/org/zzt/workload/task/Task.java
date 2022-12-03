package org.zzt.workload.task;


public class Task {
    public static TaskInterface[] tasks = new TaskInterface[] {
            new TaskInterface() {public String sql() {return Template.random1();}},
            new TaskInterface() {public String sql() {return Template.random2();}},
            new TaskInterface() {public String sql() {return Template.random3();}},
            new TaskInterface() {public String sql() {return Template.random4();}},
            new TaskInterface() {public String sql() {return Template.random5();}},
            new TaskInterface() {public String sql() {return Template.random6();}},
            new TaskInterface() {public String sql() {return Template.random7();}},
            new TaskInterface() {public String sql() {return Template.random8();}},
            new TaskInterface() {public String sql() {return Template.random9();}},
            new TaskInterface() {public String sql() {return Template.random10();}},
            new TaskInterface() {public String sql() {return Template.random11();}},
            new TaskInterface() {public String sql() {return Template.random12();}}
    };
}
