package com.guider.hadoop.tablejoin;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserAndCityBean implements WritableComparable<UserAndCityBean> {
    private String userId = "";
    private String userName = "";
    private String cityId = "";
    private String cityName = "";
    private int flag = 0;

    public UserAndCityBean() {
    }

    public UserAndCityBean(UserAndCityBean userAndCityBean) {
        this.userId = userAndCityBean.getUserId();
        this.userName = userAndCityBean.getUserName();
        this.cityId = userAndCityBean.getCityId();
        this.cityName = userAndCityBean.getCityName();
        this.flag = userAndCityBean.getFlag();
    }

    public UserAndCityBean(String userId, String userName, String cityId, String cityName, int flag) {
        this.userId = userId;
        this.userName = userName;
        this.cityId = cityId;
        this.cityName = cityName;
        this.flag = flag;
    }


    @Override
    public int compareTo(UserAndCityBean o) {
        return Integer.valueOf(userId) - Integer.valueOf(o.getUserId());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(userId);
        dataOutput.writeUTF(userName);
        dataOutput.writeUTF(cityId);
        dataOutput.writeUTF(cityName);
        dataOutput.writeInt(flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.userId = dataInput.readUTF();
        this.userName = dataInput.readUTF();
        this.cityId = dataInput.readUTF();
        this.cityName = dataInput.readUTF();
        this.flag = dataInput.readInt();
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getCityId() {
        return cityId;
    }

    public void setCityId(String cityId) {
        this.cityId = cityId;
    }

    public String getCityName() {
        return cityName;
    }

    public void setCityName(String cityName) {
        this.cityName = cityName;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return userId + ' ' + userName + ' ' + cityId + ' ' + cityName + ' ' + flag;
    }
}
