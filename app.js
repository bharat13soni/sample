var myApp = angular.module("myApp", ["ngRoute","ngAnimate"]);

myApp.config(function ($routeProvider) {
    $routeProvider
        .when("/books", {
            templateUrl: "partials/book-list.html",
            controller: "BookListCtrl"
        })
        .when("/kart", {
            templateUrl: "partials/kart-list.html",
            controller: "KartListCtrl"
        })
        .otherwise({
            redirectTo: "/books"
        });
});

myApp.factory("bookService", function () {
    var books = [
        {
            imgUrl: "adultery.jpeg",
            name: "Adultery",
            price: 205,
            rating: 4,
            binding: "Paperback",
            publisher: "Random House India",
            releaseDate: "12-08-2020",
            deatils: "Linda, in her thirties, begins to question the routine and predoctablity of her days."

        },
        {
            imgUrl: "geronimo.jpeg",
            name: "Geronimo",
            price: 205,
            rating: 4,
            binding: "Paperback",
            publisher: "Random House India",
            releaseDate: "12-08-2014",
            deatils: "Linda, in her thirties, begins to question the routine and predoctablity of her days."

        }
    ];
    return {
        getBooks: function () {
            return books;
        }

    }
});

myApp.factory("kartService", function () {
    var kart = [];
    return {
        getKart: function () {
            return kart;
        },
        addToKart: function (books) {
            kart.push(book);
        },
        buy: function (book) {
            alert("Thanks for buying: ", book);
        }
    }
});
myApp.controller("HeaderCtrl", function ($scope,$location) {
    $scope.appDetails = {};
    $scope.appDetails.title = "Bookkart";
    $scope.appDetails.tagline = "We have 1 million books for you";
    $scope.nav={};
    $scope.nav.isActive=function(path){
        if(path===$location.path()){
            return true;
        }
        return false;
    }
});

myApp.controller("BookListCtrl", function ($scope, bookService, kartService) {
    $scope.books =bookService.getBooks();
        $scope.addToKart = function (book) {
            kartService.addToKart(book);
        }

});

myApp.controller("KartListCtrl", function ($scope, kartService) {
    $scope.kart = kartService.getKart();
    $scope.buy = function (book) {
        kartService.buy(book);
    }
});