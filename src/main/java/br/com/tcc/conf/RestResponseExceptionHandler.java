package br.com.tcc.conf;


import lombok.Builder;
import lombok.Data;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;

@ControllerAdvice
public class RestResponseExceptionHandler {


        @ExceptionHandler(NotFoundException.class)
        @ResponseStatus(value= HttpStatus.NOT_FOUND)
        @ResponseBody
        public Message requestHandlingNoHandlerFound(NotFoundException exception ) {
            return Message.builder().shortMessagem("Not Found").detail(exception.getMessage()).build();
        }

        @ExceptionHandler(BadRequestException.class)
        @ResponseStatus(value= HttpStatus.NOT_FOUND)
        @ResponseBody
        public Message requestHandlingNoHandlerFound(BadRequestException exception ) {
                return Message.builder().shortMessagem("Bad Request").detail(exception.getMessage()).build();
        }

}

@Builder
@Data
class Message{
        private String shortMessagem;
        private String detail;
}