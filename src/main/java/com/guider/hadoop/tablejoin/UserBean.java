package com.guider.hadoop.tablejoin;

public class UserBean {
    private int userId;
    private String userName;
    private String address;
    private String phoneNum;

    public UserBean() {
    }

    public UserBean(int userId, String userName, String address, String phoneNum) {
        this.userId = userId;
        this.userName = userName;
        this.address = address;
        this.phoneNum = phoneNum;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getPhoneNum() {
        return phoneNum;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }

    @Override
    public String toString() {
        return userId + " " + userName + ' ' + address + ' ' +  phoneNum ;
    }
}
