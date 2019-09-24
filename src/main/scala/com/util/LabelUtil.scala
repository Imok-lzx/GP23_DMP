package com.util



object LabelUtil{
  def CaoZuo (client: Int) = {
    if (client == 1) {
      ("AndroidD00010001",1)
    }else if (client==2){
      ("IOSD00010002",1)
    }else if (client==3){
      ("WinPhoneD00010003",1)
    }else{
      ("OtherD00010004",1)
    }
  }
}
