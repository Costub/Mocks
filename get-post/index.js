

let fetchData = () => {
    let reqObj = new XMLHttpRequest()
    reqObj.open("GET", "data.json")
    reqObj.send()
    reqObj.onload = () => {
        let res = JSON.parse(reqObj.responseText)
        localStorage.setItem("users", JSON.stringify(res))
        showData()
    }
}

let showData = () => {
    let tbody = document.getElementById("tbody")
    tbody.innerHTML = ""
    let storedUser = JSON.parse(localStorage.getItem("users"))
    storedUser.map((user , index) => (
        tbody.innerHTML += `
                <tr>
                    <td>${index+1}</td>
                    <td>${user.name}</td>
                    <td>${user.email}</td>
                    <td>${user.phone}</td>
                    <td>${user.address}</td>
                    
                </tr>`
    ))
}

fetchData()

let saveData = ()=>{

    const name = document.getElementById("uname").value
    const email = document.getElementById("email").value
    const phone = document.getElementById("phoneno").value
    const address = document.getElementById("address").value

    let postObj = {
        name,email,phone,address
    };

    let obj = new XMLHttpRequest()
    obj.open("post","https://jsonplaceholder.typicode.com/users/")
    obj.setRequestHeader('content-type','application/json charset=UTF-8')
    obj.send(JSON.stringify(postObj))

    obj.onload = ()=>{
        if(obj.status == 201)
        {
            let storedUser = JSON.parse(localStorage.getItem("users"))
            storedUser.push(postObj)
            localStorage.setItem("users",JSON.stringify(storedUser))
            showData()

        }
    }


    
}