
var select = document.getElementById("dropdown");
var rest_name_to_id = {}
select.onchange = function() {
	var value = select.options[select.selectedIndex].value;
	var req = new XMLHttpRequest();

	req.onreadystatechange = function() {
	    if (this.readyState!==4) return;
	    if (this.status===200) { // HTTP 200 OK
	    	var text = this.responseText;
	    	var dict = JSON.parse(this.responseText);
	    	console.log(dict);
	    	document.getElementById("avg_rev").src = "data:image/png;base64, " + dict["avg_rev"];
	    	document.getElementById("res_rev").src = "data:image/png;base64, " + dict["res_rev"];
	    	document.getElementById("comp_score").src = "data:image/png;base64, " + dict["comp_score"];



	    } else {
	    	return;
	    }
	};
	req.open('GET', 'http://128.84.48.178:5000/img_data?' + 'id=' + rest_name_to_id[value], true);
	req.send();

}

var xhr = new XMLHttpRequest();
xhr.onreadystatechange = function() {
    if (this.readyState!==4) return;
    if (this.status===200) { // HTTP 200 OK
    	var text = this.responseText;
    	rest_name_to_id = JSON.parse(this.responseText);
    	for(var key in rest_name_to_id) {
    		var option = document.createElement('option');
    		option.text = option.value = key;
    		select.add(option,0);
    	}

    } else {
    	return;
    }
};
xhr.open('GET', 'http://128.84.48.178:5000/get_data', true);
xhr.send();


