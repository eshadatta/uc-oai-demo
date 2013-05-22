function startSearch(){
        var verb = document.getElementById('oai-form').verb.value;
        if (verb == ''){
            alert("Please pick a verb");
        }
        else{ 
            document.getElementById('oai-form').submit();
        }

}


